pub mod devices;
pub mod handler;
pub mod mixer;

pub use devices::{DeviceEvent, DeviceEventRx, DeviceState};
pub use mixer::{DeviceInfo, StandbyMode, TrackId};

use crate::aud::{
    devices::DeviceStates,
    handler::{HostHandlerError, host_handler},
    mixer::{MixerCmdError, MixerHandle, Mixers},
};
use fundsp::prelude::AudioUnit;
use my_remote_speaker::task::{TaskHandle, TaskManager, TypedTaskState};
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::{CancellationToken, DropGuard};

/// Sample Rate = 48000 Hz
pub const SAMPLE_RATE: u32 = 48000;

/// 空闲请求待机延迟。host 图无任何 track 且持续此时间后请求待机。
const STANDBY_DELAY: Duration = Duration::from_secs(2);

/// 设备从快照消失后的回收宽限。
const DEVICE_GRACE: Duration =
    Duration::from_secs((MAX_RENDER_SKIP_FRAMES / SAMPLE_RATE as usize) as u64);

// 每设备轨数上限 = GROUP_CAP * L2_CAP

/// 每级 sum adder 的轨数
const GROUP_CAP: usize = 8;
/// 二级 adder 可接的组数
const L2_CAP: usize = 8;

/// 渲染式跳过的最大帧数。超过部分不渲染。
const MAX_RENDER_SKIP_FRAMES: usize = 10 * SAMPLE_RATE as usize;

/// master 链的 limiter attack 参数
const LIMITER_ATTACK: f32 = 0.005;
/// master 链的 limiter release 参数
const LIMITER_RELEASE: f32 = 0.15;

/// 音频门面。
#[derive(Clone)]
pub struct AudioManager {
    _guard: Arc<DropGuard>,
    host_handle: Arc<TaskHandle<(), (), HostHandlerError>>,
    mixers: Arc<Mixers>,
    states: DeviceStates,
}

impl AudioManager {
    /// 启动 Audio Host 和 Mixer，等待设备就绪。
    pub async fn new(tm: TaskManager, ct: CancellationToken) -> Result<Self, AudioManagerError> {
        let mixers = Arc::new(Mixers::new(tm.clone(), ct.clone()));
        let states = DeviceStates::new();
        let mixers_for_host_handler = mixers.clone();
        let states_for_host_handler = states.clone();
        let host_handle = tm.spawn_blocking_typed(move |tm, pu, ct| {
            host_handler(tm, pu, ct, mixers_for_host_handler, states_for_host_handler)?;
            Ok::<(), HostHandlerError>(())
        });
        host_handle.cancel_at(&ct);
        ensure_host_is_running(&host_handle).await?;
        Ok(Self {
            _guard: ct.drop_guard().into(),
            host_handle: host_handle.into(),
            mixers,
            states,
        })
    }

    /// 当前设备目录快照。
    /// - 黑名单设备仍会列出，但命令会返回 [DeviceError::Gone]。
    pub fn devices(&self) -> Vec<Device> {
        let mut devs: Vec<Device> = self
            .states
            .snapshot()
            .into_iter()
            .filter_map(|(id, _entry)| {
                let handle = self.mixers.handle(&id)?;
                Some(Device::new(id, handle, self.states.clone()))
            })
            .collect();
        devs.sort_by(|a, b| a.id.cmp(&b.id));
        devs
    }

    /// 按 id 取设备视图。
    pub fn device(&self, id: &str) -> Option<Device> {
        let handle = self.mixers.handle(id)?;
        self.states.get(id)?;
        Some(Device::new(id.to_owned(), handle, self.states.clone()))
    }

    /// 订阅设备生命周期事件。
    pub fn events(&self) -> DeviceEventRx {
        self.states.events()
    }

    pub async fn wait_ready(&self) -> Result<(), AudioManagerError> {
        ensure_host_is_running(&self.host_handle).await
    }
}

/// 设备视图。
#[derive(Clone)]
pub struct Device {
    id: String,
    handle: MixerHandle,
    states: DeviceStates,
}

impl Device {
    fn new(id: String, handle: MixerHandle, states: DeviceStates) -> Self {
        Self { id, handle, states }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn info(&self) -> Option<DeviceInfo> {
        self.states.get(&self.id).map(|e| e.info)
    }

    /// 实时状态。
    pub fn state(&self) -> DeviceState {
        self.states
            .get(&self.id)
            .map(|e| e.state)
            .unwrap_or(DeviceState::Gone)
    }

    /// 把音频源挂上本设备的 mixer。
    /// - unit 必须是生成器（0 输入 2 输出）。
    /// - 传入原实例，不要 clone，会断控制通道。
    pub fn attach(&self, unit: Box<dyn AudioUnit>) -> Result<TrackId, DeviceError> {
        self.ensure_live()?;
        self.handle.attach_track(unit).map_err(|e| self.classify(e))
    }

    /// 把音频源从 mixer 摘下。返回是否实际存在并被移除。
    pub fn detach(&self, id: TrackId) -> Result<bool, DeviceError> {
        self.ensure_live()?;
        self.handle.detach_track(id).map_err(|e| self.classify(e))
    }

    /// 唤醒（已 standby 的）stream。有新内容要播时先调这个。
    pub fn resume(&self) -> Result<(), DeviceError> {
        self.ensure_live()?;
        self.handle.resume().map_err(|e| self.classify(e))
    }

    /// 设置空闲待机模式。
    pub fn set_standby_mode(&self, mode: StandbyMode) -> Result<(), DeviceError> {
        self.ensure_live()?;
        self.handle
            .set_standby_mode(mode)
            .map_err(|e| self.classify(e))
    }

    fn ensure_live(&self) -> Result<(), DeviceError> {
        ensure!(self.state() != DeviceState::Gone, GoneSnafu);
        Ok(())
    }

    fn classify(&self, e: MixerCmdError) -> DeviceError {
        match e {
            MixerCmdError::Send | MixerCmdError::Timeout if self.state() == DeviceState::Gone => {
                DeviceError::Gone
            }
            other => DeviceError::Mixer { source: other },
        }
    }
}

/// Device 命令错误。
#[derive(Snafu, Debug)]
pub enum DeviceError {
    #[snafu(display("device is gone (unplugged or removed)"))]
    Gone,
    #[snafu(display("mixer command failed: {source}"))]
    Mixer { source: MixerCmdError },
}

async fn ensure_host_is_running(
    host_handle: &TaskHandle<(), (), HostHandlerError>,
) -> Result<(), AudioManagerError> {
    let s = host_handle.wait_for(|s| s.is_running()).await;
    match s {
        TypedTaskState::Running(_) => Ok(()),
        TypedTaskState::Failed(e) => Err(AudioManagerError::HostHandler { source: e }),
        TypedTaskState::Panicked(e) => Err(AudioManagerError::HostHandlerPanicked { msg: e }),
        // Cancelled / Cancelling / Completed / Invalid
        // host 没跑起来就退了
        _ => Err(AudioManagerError::HostHandlerQuited),
    }
}

#[derive(Snafu, Debug)]
pub enum AudioManagerError {
    #[snafu(display("Audio host errored: {}", source))]
    HostHandler { source: Arc<HostHandlerError> },
    #[snafu(display("Audio host quit unexpectedly."))]
    HostHandlerQuited,
    #[snafu(display("Audio host panicked with message: {}", msg))]
    HostHandlerPanicked { msg: Arc<String> },
}

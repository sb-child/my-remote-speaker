pub mod handler;
pub mod mixer;
pub mod scheduler;

use crate::aud::{
    handler::{HostHandlerError, host_handler},
    mixer::{DeviceInfo, MixerHandle, Mixers},
};
use my_remote_speaker::task::{TaskHandle, TaskManager, TypedTaskState};
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::{CancellationToken, DropGuard};

/// Sample Rate = 48000 Hz
pub const SAMPLE_RATE: u32 = 48000;

/// 空闲请求待机延迟。host 图无任何 track 且持续此时间后请求待机。
const STANDBY_DELAY: Duration = Duration::from_secs(2);

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

#[derive(Clone)]
pub struct AudioManager {
    _guard: Arc<DropGuard>,
    host_handle: Arc<TaskHandle<(), (), HostHandlerError>>,
    mixers: Arc<Mixers>,
}

impl AudioManager {
    pub async fn new(tm: TaskManager, ct: CancellationToken) -> Result<Self, AudioManagerError> {
        let mixers = Arc::new(Mixers::new(tm.clone(), ct.clone()));
        let mixers_for_host_handler = mixers.clone();
        let host_handle = tm.spawn_blocking_typed(move |tm, pu, ct| {
            host_handler(tm, pu, ct, mixers_for_host_handler)?;
            Ok::<(), HostHandlerError>(())
        });
        host_handle.cancel_at(&ct);
        ensure_host_is_running(&host_handle).await?;
        Ok(Self {
            _guard: ct.drop_guard().into(),
            host_handle: host_handle.into(),
            mixers,
        })
    }

    pub async fn list_devices(&self) -> Result<Vec<DeviceInfo>, AudioManagerError> {
        ensure_host_is_running(&self.host_handle).await?;
        Ok(self.mixers.devices())
    }

    pub async fn get_mixer_handle(
        &self,
        device_id: &str,
    ) -> Result<MixerHandle, AudioManagerError> {
        ensure_host_is_running(&self.host_handle).await?;
        self.mixers.handle(device_id).context(MixerHandleSnafu)
    }
}

pub struct Device {
    info: DeviceInfo,
    mixers: Arc<Mixers>,
}

impl Device {
    pub fn get_handle() {}
}

async fn ensure_host_is_running(
    host_handle: &TaskHandle<(), (), HostHandlerError>,
) -> Result<(), AudioManagerError> {
    let s = host_handle.wait_for(|s| s.is_running()).await;
    ensure!(
        matches!(
            s,
            TypedTaskState::Cancelled | TypedTaskState::Cancelling | TypedTaskState::Completed(_)
        ),
        HostHandlerQuitedSnafu
    );
    match s {
        TypedTaskState::Failed(e) => {
            return Err(AudioManagerError::HostHandler { source: e });
        }
        TypedTaskState::Panicked(e) => {
            return Err(AudioManagerError::HostHandlerPanicked { msg: e });
        }
        _ => Ok(()),
    }
}

#[derive(Snafu, Debug)]
pub enum AudioManagerError {
    #[snafu(display("Audio host errored: {}", source))]
    HostHandler { source: Arc<HostHandlerError> },
    #[snafu(display("Audio host quit unexpectedly."))]
    HostHandlerQuited,
    #[snafu(display("Audio host panicked whth message: {}", msg))]
    HostHandlerPanicked { msg: Arc<String> },

    #[snafu(display("The mixer handle could not be found."))]
    MixerHandle,
}

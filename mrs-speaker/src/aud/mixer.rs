use crate::aud::{
    GROUP_CAP, L2_CAP, LIMITER_ATTACK, LIMITER_RELEASE, MAX_RENDER_SKIP_FRAMES, SAMPLE_RATE,
    STANDBY_DELAY,
};
use crossfire::{BlockingTxTrait, RecvTimeoutError, select::Select};
use dashmap::DashMap;
use fundsp::prelude::*;
use my_remote_speaker::{
    task::{TaskHandle, TaskManager},
    util::AtomicInstant,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

/// 混音器 registry
/// - 为每设备分配一个 Mixer。
pub struct Mixers {
    tm: TaskManager,
    ct: CancellationToken,
    inner: Arc<DashMap<String, MixerBundle>>,
}

/// 设备的可读信息
#[derive(Clone, Debug)]
pub struct DeviceInfo {
    /// 设备 id
    pub id: String,
    /// 设备名
    pub name: String,
    /// 物理地址/连接标识
    pub address: Option<String>,
}

impl DeviceInfo {
    pub fn create(dev_id_str: &str, desc: Option<&cpal::DeviceDescription>) -> Self {
        Self {
            id: dev_id_str.to_owned(),
            name: desc.map(|d| d.name().to_owned()).unwrap_or_default(),
            address: desc.and_then(|d| d.address().map(str::to_owned)),
        }
    }
}

impl Mixers {
    pub fn new(tm: TaskManager, ct: CancellationToken) -> Self {
        Self {
            tm,
            ct,
            inner: Arc::new(DashMap::new()),
        }
    }

    /// 取设备的 mixer，不存在则创建。
    pub(crate) fn get_or_create(
        &self,
        info: &DeviceInfo,
    ) -> (MixerHandle, MixerController, MixerOutput) {
        if self
            .inner
            .remove_if(&info.id, |_, v| worker_panicked(v))
            .is_some()
        {
            warn!(dev = %info.id, "mixer worker panicked. dropping and recreating.");
        }
        if let Some(b) = self.inner.get(&info.id) {
            return (b.handle.clone(), b.ctrl.clone(), b.out.clone());
        }
        let (mixer, handle, ctrl, out) = Mixer::create(&self.tm, &self.ct, &info.id);
        let ret = (handle.clone(), ctrl.clone(), out.clone());
        self.inner.insert(
            info.id.clone(),
            MixerBundle {
                mixer,
                handle,
                ctrl,
                out,
                meta: info.clone(),
            },
        );
        ret
    }

    /// 移除指定设备的 Mixer。
    pub(crate) fn remove(&self, dev_id: &str) {
        if self.inner.remove(dev_id).is_some() {
            info!(dev = %dev_id, "mixer removed");
        }
    }

    /// 获取指定设备的 MixerHandle。
    pub fn handle(&self, dev_id: &str) -> Option<MixerHandle> {
        self.inner.get(dev_id).map(|b| b.handle.clone())
    }

    /// 枚举当前的设备。
    pub fn devices(&self) -> Vec<DeviceInfo> {
        self.inner.iter().map(|b| b.meta.clone()).collect()
    }
}

fn worker_panicked(b: &MixerBundle) -> bool {
    b.mixer
        .worker
        .as_ref()
        .map_or(false, |w| w.status().is_panicked())
}

/// mixer 到 device 的控制事件
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MixerEvent {
    /// 请求暂停流。
    RequestStandby,
    /// 请求恢复流。
    RequestResume,
}

type MixerEventTx = crossfire::MTx<crossfire::mpmc::Array<MixerEvent>>;
pub type MixerEventRx = crossfire::MRx<crossfire::mpmc::Array<MixerEvent>>;

/// 空闲待机模式
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StandbyMode {
    /// 默认：输出空闲（低电平）`STANDBY_DELAY` 后请求暂停 stream，有内容时由调用方 resume。
    Auto,
    /// 强制 stream 一直保持运行
    ForcePlay,
}

impl Default for StandbyMode {
    fn default() -> Self {
        Self::Auto
    }
}

/// master 输出链
/// - 输入 -> DC blocker -> look-ahead limiter -> 输出。
fn master_chain() -> Box<dyn AudioUnit> {
    Box::new(
        (dcblock::<f32>() | dcblock::<f32>()) >> limiter_stereo(LIMITER_ATTACK, LIMITER_RELEASE),
    )
}

/// 求和层
/// - 输入 cap 组立体声输入信号（cap * 2 个通道）-> 逐通道累加合并 -> 输出 1 组立体声信号（2 个通道）
#[derive(Clone)]
struct SumLevel {
    pairs: usize,
}

impl AudioUnit for SumLevel {
    fn tick(&mut self, input: &[f32], output: &mut [f32]) {
        output[0] = 0.0; // 左声道输出采样点
        output[1] = 0.0; // 右声道输出采样点
        for i in 0..self.pairs {
            output[0] += input[i * 2]; // 左声道累加
            output[1] += input[i * 2 + 1]; // 右声道累加
        }
    }

    fn process(&mut self, size: usize, input: &BufferRef, output: &mut BufferMut) {
        debug_assert_eq!(input.channels(), self.pairs * 2);
        debug_assert!(size <= MAX_BUFFER_SIZE);
        for s in 0..size {
            let (mut l, mut r) = (0.0f32, 0.0f32);
            for c in 0..self.pairs {
                l += input.channel_f32(c * 2)[s]; // 累加采样点 s 处的左声道
                r += input.channel_f32(c * 2 + 1)[s]; // 累加采样点 s 处的右声道
            }
            output.channel_f32_mut(0)[s] = l;
            output.channel_f32_mut(1)[s] = r;
        }
    }

    fn inputs(&self) -> usize {
        self.pairs * 2
    }

    fn outputs(&self) -> usize {
        2
    }

    fn route(&mut self, _input: &SignalFrame, _frequency: f64) -> SignalFrame {
        SignalFrame::new(2)
    }

    fn get_id(&self) -> u64 {
        // "MRSS": Multi-Route Stereo Summer
        0x4d52_5353
    }

    fn footprint(&self) -> usize {
        0
    }
}

type OneshotTx<T> = crossfire::oneshot::TxOneshot<T>;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, Default)]
pub struct TrackId(NodeId);

impl From<NodeId> for TrackId {
    fn from(value: NodeId) -> Self {
        Self(value)
    }
}

impl Into<NodeId> for TrackId {
    fn into(self) -> NodeId {
        self.0
    }
}

/// attach 校验错误
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    #[error("Track 必须是生成器(0 输入)，实际 {inputs} 输入")]
    NotGenerator { inputs: usize },
    #[error("Track 必须输出 2 声道，实际 {outputs} 声道")]
    NotStereo { outputs: usize },
    #[error("Mixer 的 sum 树已满(上限 {cap} Track)")]
    NoSlot { cap: usize },
}

/// mixer worker 命令
pub enum MixerCmd {
    Attach {
        unit: Box<dyn AudioUnit>,
        tx: OneshotTx<Result<TrackId, AttachError>>,
    },
    Detach {
        id: TrackId,
        tx: OneshotTx<bool>,
    },
    SetStandbyMode(StandbyMode, OneshotTx<()>),
    Resume((), OneshotTx<()>),
}

type MixerCmdTx = crossfire::MTx<crossfire::mpmc::Array<MixerCmd>>;
type MixerCmdRx = crossfire::MRx<crossfire::mpmc::Array<MixerCmd>>;

struct MixerBundle {
    mixer: Mixer,
    handle: MixerHandle,
    ctrl: MixerController,
    out: MixerOutput,
    meta: DeviceInfo,
}

/// 混音器宿主（每设备一个）
pub struct Mixer {
    worker: Option<TaskHandle<(), (), ()>>,
    _ct_guard: DropGuard,
}

impl Mixer {
    fn create(
        tm: &TaskManager,
        ct: &CancellationToken,
        dev_id: &str,
    ) -> (Self, MixerHandle, MixerController, MixerOutput) {
        let mixer_ct = ct.child_token();
        let (cmd_tx, cmd_rx) = crossfire::mpmc::bounded_blocking(16);
        let (events_tx, events_rx) = crossfire::mpmc::bounded_blocking(16);
        let (backend_tx, backend_rx) = crossfire::oneshot::oneshot();
        let state = Arc::new(MixerLinkState {
            backend_errored: Default::default(),
            track_count: Default::default(),
            disconnected: Default::default(),
            disconnect_at: AtomicInstant::new(Instant::now()),
            mode: Mutex::new(StandbyMode::Auto),
            events_tx: events_tx.clone(),
            events_rx: events_rx.clone(),
        });
        let worker = spawn_mixer_worker(
            tm,
            cmd_rx.clone(),
            events_tx,
            state.clone(),
            backend_tx,
            &mixer_ct,
        );
        let ct_guard = mixer_ct.drop_guard();
        // 等 worker 建好 network backend。
        let backend = backend_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("mixer worker failed to initialize network within 5s");
        (
            Self {
                worker: Some(worker),
                _ct_guard: ct_guard,
            },
            MixerHandle { cmd_tx },
            MixerController {
                state: state.clone(),
            },
            MixerOutput {
                backend: Arc::new(Mutex::new(backend)),
                state,
                dev: dev_id.to_owned(),
                idle_since: None,
                standby_fired: false,
            },
        )
    }
}

fn spawn_mixer_worker(
    tm: &TaskManager,
    cmd_rx: MixerCmdRx,
    events_tx: MixerEventTx,
    state: Arc<MixerLinkState>,
    backend_tx: OneshotTx<NetBackend>,
    ct: &CancellationToken,
) -> TaskHandle<(), (), ()> {
    let h = tm.spawn_blocking_typed(move |_tm, pc, ct| {
        pc.update(());
        mixer_worker(cmd_rx, events_tx, state, backend_tx, ct);
        Ok(())
    });
    h.cancel_at(ct);
    h
}

/// 一级 sum 组
struct SumGroup {
    node: NodeId,
    /// 每个端口对应的 child 节点
    slots: Vec<Option<NodeId>>,
}

/// 开一组一级 adder 并接到二级 adder 的下一个空闲口。
fn open_group(net: &mut Net, l2: &NodeId, groups: &mut Vec<SumGroup>) {
    let node = net.push(Box::new(SumLevel { pairs: GROUP_CAP }));
    let idx = groups.len();
    net.connect(node, 0, *l2, idx * 2);
    net.connect(node, 1, *l2, idx * 2 + 1);
    groups.push(SumGroup {
        node,
        slots: vec![None; GROUP_CAP],
    });
}

/// worker 主循环
fn mixer_worker(
    cmd_rx: MixerCmdRx,
    events_tx: MixerEventTx,
    state: Arc<MixerLinkState>,
    backend_tx: OneshotTx<NetBackend>,
    ct: CancellationToken,
) {
    let mut net = Net::new(0, 2);
    let master = net.chain(master_chain());
    let l2 = net.push(Box::new(SumLevel { pairs: L2_CAP }));
    net.connect(l2, 0, master, 0);
    net.connect(l2, 1, master, 1);
    let mut groups: Vec<SumGroup> = Vec::new();
    open_group(&mut net, &l2, &mut groups); // 第一组
    net.set_sample_rate(SAMPLE_RATE as f64);
    net.check();

    // 把 backend 发给 MixerOutput
    let mut backend = net.backend();
    backend.set_sample_rate(SAMPLE_RATE as f64);
    backend.allocate();
    let _ = backend_tx.send(backend);

    // 命令循环
    let mut sel = Select::new();
    sel.add(&cmd_rx);
    loop {
        if ct.is_cancelled() {
            return;
        }
        match sel.select_timeout(Duration::from_millis(50)) {
            Ok(res) => {
                if res == cmd_rx {
                    match cmd_rx.read_select(res) {
                        Ok(cmd) => handle_cmd(cmd, &mut net, &l2, &mut groups, &events_tx, &state),
                        Err(_) => sel.remove(&cmd_rx),
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

fn handle_cmd(
    cmd: MixerCmd,
    net: &mut Net,
    l2: &NodeId,
    groups: &mut Vec<SumGroup>,
    events_tx: &MixerEventTx,
    state: &Arc<MixerLinkState>,
) {
    match cmd {
        MixerCmd::Attach { unit, tx } => {
            let r = attach_child(net, l2, groups, unit);
            if r.is_ok() {
                state.track_count.fetch_add(1, Ordering::Relaxed);
            }
            tx.send(r);
        }
        MixerCmd::Detach { id, tx } => {
            let removed = detach_child(net, groups, id);
            if removed {
                state.track_count.fetch_sub(1, Ordering::Relaxed);
            }
            tx.send(removed);
        }
        MixerCmd::SetStandbyMode(mode, tx) => {
            if let Ok(mut m) = state.mode.lock() {
                *m = mode;
            }
            tx.send(());
        }
        MixerCmd::Resume(_, tx) => {
            // 向 device 发送唤醒信号
            let _ = events_tx.send(MixerEvent::RequestResume);
            tx.send(());
        }
    }
}

/// 找一个空槽。优先复用已有组的空口，否则开新组。
fn find_slot(net: &mut Net, l2: &NodeId, groups: &mut Vec<SumGroup>) -> Option<(usize, usize)> {
    for (gi, g) in groups.iter_mut().enumerate() {
        if let Some(si) = g.slots.iter().position(Option::is_none) {
            return Some((gi, si));
        }
    }
    if groups.len() >= L2_CAP {
        return None; // 二级口用完
    }
    open_group(net, l2, groups);
    let gi = groups.len() - 1;
    Some((gi, 0))
}

/// 插入 child。
/// - 应插入原实例。不要 clone child，会断连控制通道。
fn attach_child(
    net: &mut Net,
    l2: &NodeId,
    groups: &mut Vec<SumGroup>,
    unit: Box<dyn AudioUnit>,
) -> Result<TrackId, AttachError> {
    let inputs = unit.inputs();
    if inputs != 0 {
        return Err(AttachError::NotGenerator { inputs });
    }
    let outputs = unit.outputs();
    if outputs != 2 {
        return Err(AttachError::NotStereo { outputs });
    }
    let (gi, si) = find_slot(net, l2, groups).ok_or(AttachError::NoSlot {
        cap: GROUP_CAP * L2_CAP,
    })?;
    let node = net.push(unit);
    let g = &groups[gi];
    net.connect(node, 0, g.node, si * 2);
    net.connect(node, 1, g.node, si * 2 + 1);
    groups[gi].slots[si] = Some(node);
    net.commit();
    Ok(node.into())
}

/// 摘下 child，断开接线并释放槽位。
fn detach_child(net: &mut Net, groups: &mut Vec<SumGroup>, id: TrackId) -> bool {
    let mut found = false;
    for g in groups.iter_mut() {
        for slot in g.slots.iter_mut() {
            if *slot == Some(id.into()) {
                *slot = None;
                found = true;
            }
        }
    }
    if !found {
        return false;
    }
    net.remove(id.into());
    net.commit();
    true
}

/// MixerHandle 命令错误
#[derive(Error, Debug, PartialEq, Eq)]
pub enum MixerCmdError {
    /// 命令通道积压或断开
    #[error("command channel send failed")]
    Send,
    /// mixer worker 无响应
    #[error("mixer worker timeout")]
    Timeout,
    /// attach 校验失败
    #[error("attach rejected: {0}")]
    Attach(#[from] AttachError),
}

#[derive(Clone)]
pub struct MixerHandle {
    cmd_tx: MixerCmdTx,
}

impl MixerHandle {
    /// 设置空闲待机模式。
    /// - Auto：输出空闲超时自动待机。
    /// - ForcePlay：保持播放不待机。
    pub fn set_standby_mode(&self, mode: StandbyMode) -> Result<(), MixerCmdError> {
        let (tx, rx) = crossfire::oneshot::oneshot();
        self.cmd_tx
            .send_timeout(MixerCmd::SetStandbyMode(mode, tx), Duration::from_secs(1))
            .map_err(|_| MixerCmdError::Send)?;
        rx.recv_timeout(Duration::from_secs(1))
            .map(|_| ())
            .map_err(|_| MixerCmdError::Timeout)
    }

    /// 唤醒（已 standby 的）stream。有新内容要播时先调这个。
    pub fn resume(&self) -> Result<(), MixerCmdError> {
        let (tx, rx) = crossfire::oneshot::oneshot();
        self.cmd_tx
            .send_timeout(MixerCmd::Resume((), tx), Duration::from_secs(1))
            .map_err(|_| MixerCmdError::Send)?;
        rx.recv_timeout(Duration::from_secs(1))
            .map(|_| ())
            .map_err(|_| MixerCmdError::Timeout)
    }

    /// 把音频源挂上 mixer。返回 [TrackId]。
    /// - unit 必须是生成器（0 输入，2 输出）的 AudioUnit。
    /// - 应插入原实例。不要 clone unit，会断连控制通道。
    pub fn attach_track(&self, unit: Box<dyn AudioUnit>) -> Result<TrackId, MixerCmdError> {
        let (tx, rx) = crossfire::oneshot::oneshot();
        self.cmd_tx
            .send_timeout(MixerCmd::Attach { unit, tx }, Duration::from_secs(1))
            .map_err(|_| MixerCmdError::Send)?;
        rx.recv_timeout(Duration::from_secs(1))
            .map_err(|_| MixerCmdError::Timeout)?
            .map_err(MixerCmdError::from)
    }

    /// 把音频源从 mixer 摘下。返回是否实际存在并被移除。
    pub fn detach_track(&self, id: TrackId) -> Result<bool, MixerCmdError> {
        let (tx, rx) = crossfire::oneshot::oneshot();
        self.cmd_tx
            .send_timeout(MixerCmd::Detach { id, tx }, Duration::from_secs(1))
            .map_err(|_| MixerCmdError::Send)?;
        rx.recv_timeout(Duration::from_secs(1))
            .map_err(|_| MixerCmdError::Timeout)
    }
}

/// Mixer 链路状态。
/// - MixerOutput 和 MixerController 共享。
struct MixerLinkState {
    // 后端是否发生错误。
    // - 如果发生错误，stream 会静音。
    backend_errored: AtomicBool,
    // Mixer 图中 track 数。
    track_count: AtomicUsize,
    // stream 是否已断开连接。
    disconnected: AtomicBool,
    // 断开连接的时间。
    disconnect_at: AtomicInstant,
    /// 当前待机模式。
    /// - worker 写，output 读。
    mode: Mutex<StandbyMode>,
    events_tx: MixerEventTx,
    events_rx: MixerEventRx,
}

#[derive(Clone)]
pub struct MixerController {
    state: Arc<MixerLinkState>,
}

impl MixerController {
    pub fn reset(&self) {
        self.state.backend_errored.store(false, Ordering::Relaxed);
    }

    /// 在 stream 掉线时调用。
    /// - 恢复后 MixerOutput 会渲染式跳过断连时长。
    pub fn disconnected(&self) {
        if self.state.disconnected.swap(true, Ordering::Release) {
            return; // 只记录第一次断开连接时间
        }
        self.state
            .disconnect_at
            .store(Instant::now(), Ordering::Release);
    }

    /// 订阅 mixer 控制事件
    pub fn events(&self) -> MixerEventRx {
        self.state.events_rx.clone()
    }
}

/// Mixer 输出端。
/// - 持有 mixer network 的 NetBackend，由设备 callback 逐帧驱动。
/// - 设计上只允许一个线程读取。
#[derive(Clone)]
pub struct MixerOutput {
    backend: Arc<Mutex<NetBackend>>,
    state: Arc<MixerLinkState>,
    /// 设备 id
    dev: String,
    /// 空闲计时
    idle_since: Option<Instant>,
    standby_fired: bool,
}

impl MixerOutput {
    /// 读一帧输出
    /// - 2ch interleaved f32。
    /// - `data.len()` 必须是偶数。
    pub fn read_frames(&mut self, data: &mut [f32]) -> usize {
        if data.len() % 2 != 0 || data.is_empty() {
            return 0;
        }
        if self.state.backend_errored.load(Ordering::Relaxed) {
            return 0;
        }
        let mut backend = match self.backend.lock() {
            Ok(g) => g,
            Err(_) => {
                self.state.backend_errored.store(true, Ordering::Relaxed);
                return 0;
            }
        };

        // 断连追赶
        if self.state.disconnected.swap(false, Ordering::Acquire) {
            let gap =
                Instant::now().duration_since(self.state.disconnect_at.load(Ordering::Acquire));
            let frames = (gap.as_secs_f64() * SAMPLE_RATE as f64) as usize;
            let frames = if frames > MAX_RENDER_SKIP_FRAMES {
                MAX_RENDER_SKIP_FRAMES
            } else {
                frames
            };
            if frames > 0 {
                warn!(dev = %self.dev, frames, "rendering skip after disconnect");
                let mut scratch = [0.0f32; 2];
                for _ in 0..frames {
                    backend.tick(&[], &mut scratch);
                }
            }
        }

        // 渲染输出帧
        for frame in data.chunks_exact_mut(2) {
            let mut out = [0.0f32; 2];
            backend.tick(&[], &mut out);
            frame[0] = out[0];
            frame[1] = out[1];
        }

        // 空闲检测
        let mode = self
            .state
            .mode
            .lock()
            .map(|m| *m)
            .unwrap_or(StandbyMode::ForcePlay);
        if mode == StandbyMode::Auto {
            if self.state.track_count.load(Ordering::Relaxed) == 0 {
                let now = Instant::now();
                let since = *self.idle_since.get_or_insert(now);
                if !self.standby_fired && now.duration_since(since) >= STANDBY_DELAY {
                    self.standby_fired = true;
                    info!(dev = %self.dev, "no track for a while, requesting standby.");
                    let _ = self.state.events_tx.try_send(MixerEvent::RequestStandby);
                }
            } else {
                self.idle_since = None;
                self.standby_fired = false;
            }
        }
        data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use my_remote_speaker::task::TaskManager;
    use tokio_util::sync::CancellationToken;

    fn new_mixers(dev: &str) -> (Mixers, MixerHandle, MixerController, MixerOutput) {
        let tm = TaskManager::new();
        let ct = CancellationToken::new();
        let mixers = Mixers::new(tm, ct);
        let (handle, ctrl, out) = mixers.get_or_create(&DeviceInfo::create(dev, None));
        (mixers, handle, ctrl, out)
    }

    /// 440Hz 正弦 child（backend 原实例）
    fn sine_child(amp: f32) -> Box<dyn AudioUnit> {
        let mut net = Net::new(0, 2);
        net.chain(Box::new(
            (sine_hz::<f32>(440.0) | sine_hz::<f32>(440.0)) * amp,
        ));
        net.set_sample_rate(SAMPLE_RATE as f64);
        Box::new(net.backend())
    }

    fn read_peak(out: &mut MixerOutput, frames: usize) -> f32 {
        let mut buf = vec![0.0f32; frames * 2];
        let n = out.read_frames(&mut buf);
        assert_eq!(n, frames * 2);
        buf.iter().fold(0.0f32, |a, &v| a.max(v.abs()))
    }

    /// 全链路：attach 出声 → detach 静音（预热排空 limiter lookahead + dcblock 尾巴）
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn attach_detach_route() {
        let (_mixers, handle, _ctrl, mut out) = new_mixers("test-device");

        let id = handle.attach_track(sine_child(0.25)).unwrap();
        let p = read_peak(&mut out, 480);
        assert!(
            (0.2..0.26).contains(&p),
            "sine 0.25 should pass through, peak={p}"
        );

        assert!(handle.detach_track(id).unwrap());
        assert!(
            !handle.detach_track(id).unwrap(),
            "second detach should be false"
        );

        // 预热：limiter 排空 240 帧 + dcblock 尾巴衰减（时间常数 ~16ms，需 ~100ms 到 1e-3 下）
        for _ in 0..10 {
            let _ = read_peak(&mut out, 480);
        }
        let p = read_peak(&mut out, 480);
        assert!(p < 1e-3, "fully silent after drain, peak={p}");
    }

    /// attach 校验：非生成器 / 非立体声拒绝
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn attach_validation() {
        let (_mixers, handle, _ctrl, _out) = new_mixers("test-device");

        // 1 输入 1 输出：不是生成器
        let mut net = Net::new(1, 1);
        net.chain(Box::new(pass()));
        net.set_sample_rate(SAMPLE_RATE as f64);
        let err = handle.attach_track(Box::new(net.backend())).unwrap_err();
        assert!(matches!(
            err,
            MixerCmdError::Attach(AttachError::NotGenerator { inputs: 1 })
        ));

        // 0 输入 1 输出：非立体声
        let err = handle.attach_track(Box::new(zero())).unwrap_err();
        assert!(matches!(
            err,
            MixerCmdError::Attach(AttachError::NotStereo { outputs: 1 })
        ));
    }

    /// sum 树扩容：超过 GROUP_CAP 后自动开组，全部出声；摘掉中间轨不影响其它轨
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sum_tree_expands() {
        let (_mixers, handle, _ctrl, mut out) = new_mixers("test-device");

        let mut ids = Vec::new();
        for i in 0..GROUP_CAP + 1 {
            // 不同频率防同相叠加过激
            let freq = [
                440.0, 660.0, 880.0, 220.0, 330.0, 550.0, 770.0, 990.0, 110.0,
            ][i];
            let mut net = Net::new(0, 2);
            net.chain(Box::new(
                (sine_hz::<f32>(freq) | sine_hz::<f32>(freq)) * 0.05,
            ));
            net.set_sample_rate(SAMPLE_RATE as f64);
            let id = handle.attach_track(Box::new(net.backend())).unwrap();
            ids.push(id);
        }
        // 9 轨（> GROUP_CAP=8）都 attach 成功，输出有声音
        let p = read_peak(&mut out, 480);
        assert!(p > 0.1, "sum of 9 tracks should be audible, peak={p}");

        // 摘掉第一轨和中间一轨
        assert!(handle.detach_track(ids[0]).unwrap());
        assert!(handle.detach_track(ids[4]).unwrap());
        let p = read_peak(&mut out, 480);
        assert!(p > 0.05, "remaining tracks audible, peak={p}");

        // 全部摘掉 → 静音
        for id in &ids {
            let _ = handle.detach_track(*id).unwrap();
        }
        for _ in 0..12 {
            let _ = read_peak(&mut out, 480); // 预热排空
        }
        let p = read_peak(&mut out, 480);
        assert!(p < 1e-3, "all detached -> silent, peak={p}");
    }

    /// Auto 模式：空图读够 STANDBY_DELAY 后收到 RequestStandby
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn standby_requested_when_idle() {
        let (_mixers, handle, ctrl, mut out) = new_mixers("test-device");
        let rx = ctrl.events();

        // 空图（无 child）= 静音；循环读模拟 callback
        let deadline = Instant::now() + Duration::from_secs(4);
        let mut got = false;
        while Instant::now() < deadline {
            let _ = read_peak(&mut out, 96);
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(MixerEvent::RequestStandby) => {
                    got = true;
                    break;
                }
                _ => {}
            }
        }
        assert!(got, "should request standby after idle delay");

        // ForcePlay 模式不应再发（避免旧事件残留：先读空）
        handle.set_standby_mode(StandbyMode::ForcePlay).unwrap();
        while rx.recv_timeout(Duration::ZERO).is_ok() {}
        let _ = read_peak(&mut out, 96);
        // 读几轮后不应有新事件
        for _ in 0..5 {
            let _ = read_peak(&mut out, 96);
            assert!(
                !matches!(
                    rx.recv_timeout(Duration::ZERO),
                    Ok(MixerEvent::RequestStandby)
                ),
                "ForcePlay must not request standby"
            );
        }
    }

    /// 有内容时不应触发 standby；内容结束后 idle 计时重新开始
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_standby_while_playing() {
        let (_mixers, handle, ctrl, mut out) = new_mixers("test-device");
        let mut rx = ctrl.events();
        handle.set_standby_mode(StandbyMode::Auto).unwrap();

        let id = handle.attach_track(sine_child(0.2)).unwrap();
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(600) {
            let _ = read_peak(&mut out, 96);
            assert!(
                !matches!(
                    rx.recv_timeout(Duration::ZERO),
                    Ok(MixerEvent::RequestStandby)
                ),
                "no standby while playing"
            );
        }
        handle.detach_track(id).unwrap();

        // 结束后 idle 计时（2s）内仍不应立刻触发
        // （先预热排空 limiter/dcblock 尾巴再断言内容已停）
        for _ in 0..5 {
            let _ = read_peak(&mut out, 96);
        }
        let p = read_peak(&mut out, 96);
        assert!(p < 0.1, "content stopped, peak={p}");
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(500) {
            let _ = read_peak(&mut out, 96);
        }
        assert!(!matches!(
            rx.recv_timeout(Duration::ZERO),
            Ok(MixerEvent::RequestStandby)
        ));
    }

    /// 断连恢复：渲染式跳过路径不崩、随后输出正常
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_resume_keeps_playing() {
        let (_mixers, handle, ctrl, mut out) = new_mixers("test-device");
        let id = handle.attach_track(sine_child(0.2)).unwrap();
        let p = read_peak(&mut out, 480);
        assert!(p > 0.1);

        // 模拟掉线：gap 150ms → 下一次 read 渲染跳过 ~7200 帧
        ctrl.disconnected();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let p = read_peak(&mut out, 480);
        assert!(p > 0.1, "still playing after disconnect skip, peak={p}");

        // 多轮读稳定
        for _ in 0..5 {
            let p = read_peak(&mut out, 480);
            assert!(p > 0.1);
        }
        handle.detach_track(id).unwrap();
    }

    /// 回归：播放中的内容即使整段静音（如音频内嵌长静音段），也不应触发待机；
    /// detach 后（内容真正结束）才在 STANDBY_DELAY 后触发。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn silent_track_does_not_standby() {
        let (_mixers, handle, ctrl, mut out) = new_mixers("test-device");
        let rx = ctrl.events();
        handle.set_standby_mode(StandbyMode::Auto).unwrap();

        // 恒 0 输出的 track：旧的输出电平判定会误判 idle；现按 track 存在性判定
        let mut net = Net::new(0, 2);
        net.chain(Box::new(dc((0.0f32, 0.0f32))));
        net.set_sample_rate(SAMPLE_RATE as f64);
        let id = handle.attach_track(Box::new(net.backend())).unwrap();

        // 读满 STANDBY_DELAY + 余量，期间不应有任何待机事件
        let start = Instant::now();
        while start.elapsed() < STANDBY_DELAY + Duration::from_millis(300) {
            let _ = read_peak(&mut out, 96);
            assert!(
                !matches!(
                    rx.recv_timeout(Duration::ZERO),
                    Ok(MixerEvent::RequestStandby)
                ),
                "silent but attached track must not request standby"
            );
        }

        // detach（内容结束）→ 空图 → STANDBY_DELAY 后应触发
        assert!(handle.detach_track(id).unwrap());
        let deadline = Instant::now() + STANDBY_DELAY + Duration::from_secs(1);
        let mut got = false;
        while Instant::now() < deadline {
            let _ = read_peak(&mut out, 96);
            if matches!(
                rx.recv_timeout(Duration::ZERO),
                Ok(MixerEvent::RequestStandby)
            ) {
                got = true;
                break;
            }
        }
        assert!(got, "standby should fire after detach + idle delay");
    }
}

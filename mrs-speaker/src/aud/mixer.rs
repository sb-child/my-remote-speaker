use crate::aud::SAMPLE_RATE;
use crossfire::{
    BlockingRxTrait, BlockingTxTrait, RecvError, RecvTimeoutError, SendTimeoutError, select::Select,
};
use dashmap::DashMap;
use my_remote_speaker::{
    task::{TaskHandle, TaskManager},
    use_id,
    util::AtomicInstant,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{error, info};

/// 混音器管理器
///
/// 为每个设备分配一个 Mixer。
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

    /// 取设备的 mixer，不存在则创建
    pub fn get_or_create(&self, info: &DeviceInfo) -> (MixerHandle, MixerController, MixerOutput) {
        if self
            .inner
            .remove_if(&info.id, |_, v| worker_panicked(v))
            .is_some()
        {
            error!(dev = %info.id, "mixer worker panicked. dropping and recreating.");
        }
        if let Some(b) = self.inner.get(&info.id) {
            return (b.handle.clone(), b.ctrl.clone(), b.out.clone());
        }
        let (mixer, handle, ctrl, out) = Mixer::create(&self.tm, &self.ct);
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

    /// 移除指定设备的 Mixer
    pub fn remove(&self, dev_id: &str) {
        if self.inner.remove(dev_id).is_some() {
            info!(dev = %dev_id, "mixer removed");
        }
    }

    /// 获取指定设备的 MixerHandle
    pub fn handle(&self, dev_id: &str) -> Option<MixerHandle> {
        self.inner.get(dev_id).map(|b| b.handle.clone())
    }

    /// 枚举当前的设备
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

use_id!(Track);

type OneshotTx<T> = crossfire::oneshot::TxOneshot<T>;

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
    /// 默认所有轨道空闲`STANDBY_DELAY`时暂停 stream，在轨道有内容时恢复。
    Auto,
    /// 强制 stream 一直保持运行
    ForcePlay,
}

impl Default for StandbyMode {
    fn default() -> Self {
        Self::Auto
    }
}

/// 空闲请求待机延迟
const STANDBY_DELAY: Duration = Duration::from_secs(2);

/// mixer worker 的空闲/待机状态机
#[derive(Default)]
struct StandbyState {
    mode: StandbyMode,
    /// 连续空闲的开始时刻
    idle_since: Option<Instant>,
    /// 已触发过待机请求
    fired: bool,
}

pub enum MixerCmd {
    AddTracks(Vec<Track>, OneshotTx<Vec<TrackId>>),
    RemoveTrack(Vec<TrackId>, OneshotTx<Vec<bool>>),
    SetStandbyMode(StandbyMode, OneshotTx<()>),
    Resume((), OneshotTx<()>),
}

type MixerCmdTx = crossfire::MTx<crossfire::mpmc::Array<MixerCmd>>;
type MixerCmdRx = crossfire::MRx<crossfire::mpmc::Array<MixerCmd>>;

enum MixerTriggerPayload {
    Seek(usize),
    Read(usize, Vec<f32>, OneshotTx<Vec<f32>>),
}

type MixerTrigRx = crossfire::MRx<crossfire::mpmc::Array<MixerTriggerPayload>>;

struct MixerBundle {
    mixer: Mixer,
    handle: MixerHandle,
    ctrl: MixerController,
    out: MixerOutput,
    meta: DeviceInfo,
}

/// 混音器
///
/// 可以挂载多个轨道，合并为一个输出。
pub struct Mixer {
    worker: Option<TaskHandle<(), (), ()>>,
    _ct_guard: DropGuard,
}

impl Mixer {
    fn create(
        tm: &TaskManager,
        ct: &CancellationToken,
    ) -> (Self, MixerHandle, MixerController, MixerOutput) {
        let mixer_ct = ct.child_token();
        let (trig_tx, trig_rx) = crossfire::mpmc::bounded_blocking(1);
        let (cmd_tx, cmd_rx) = crossfire::mpmc::bounded_blocking(16);
        let (events_tx, events_rx) = crossfire::mpmc::bounded_blocking(16);
        let worker = spawn_mixer_worker(
            tm,
            trig_rx.clone(),
            cmd_rx.clone(),
            events_tx.clone(),
            &mixer_ct,
        );
        let ct_guard = mixer_ct.drop_guard();
        let state = Arc::new(MixerLinkState {
            read_frames_errored: Default::default(),
            disconnected: Default::default(),
            disconnect_at: AtomicInstant::new(Instant::now()),
            _events_tx: events_tx,
            events_rx,
        });
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
                trig_tx,
                buf: None,
                state,
            },
        )
    }
}

fn spawn_mixer_worker(
    tm: &TaskManager,
    trig_rx: MixerTrigRx,
    cmd_rx: MixerCmdRx,
    events_tx: MixerEventTx,
    ct: &CancellationToken,
) -> TaskHandle<(), (), ()> {
    let h = tm.spawn_blocking_typed(move |pc, ct| {
        pc.update(());
        mixer_worker(trig_rx, cmd_rx, events_tx, ct);
        Ok(())
    });
    h.cancel_at(ct);
    h
}

fn mixer_worker(
    trig_rx: MixerTrigRx,
    cmd_rx: MixerCmdRx,
    events_tx: MixerEventTx,
    ct: CancellationToken,
) {
    let mut tracks: HashMap<TrackId, Track> = HashMap::new();
    let track_counter = TrackIdCounter::default();
    let mut mixer_buf = Vec::new();
    let mut standby = StandbyState::default();
    let mut sel = Select::new();
    sel.add(&trig_rx);
    sel.add(&cmd_rx);
    loop {
        match sel.select_timeout(Duration::from_millis(50)) {
            Ok(res) => {
                let read_happened = select_mixer_channel(
                    &trig_rx,
                    &cmd_rx,
                    &mut sel,
                    res,
                    &mut tracks,
                    &mut mixer_buf,
                    &track_counter,
                    &events_tx,
                    &mut standby,
                );
                mixer_update_idle(read_happened, &tracks, &mut standby, &events_tx);
                if ct.is_cancelled() {
                    return;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if ct.is_cancelled() {
                    return;
                }
            }
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// 消费一个通道事件。返回 true 表示本次处理了一个 Read（混音完成）。
fn select_mixer_channel(
    trig_rx: &MixerTrigRx,
    cmd_rx: &MixerCmdRx,
    sel: &mut crossfire::select::Select<'_>,
    res: crossfire::select::SelectResult,
    tracks: &mut HashMap<TrackId, Track>,
    mixer_buf: &mut Vec<f32>,
    track_counter: &TrackIdCounter,
    events_tx: &MixerEventTx,
    standby: &mut StandbyState,
) -> bool {
    if res == *trig_rx {
        match trig_rx.read_select(res) {
            Ok(MixerTriggerPayload::Read(size, mut buf, frame_tx)) => {
                buf.resize(size, 0.0);
                mixer_buf.clear();
                mixer_buf.reserve(size.saturating_sub(mixer_buf.capacity()));
                mixer_buf.resize(size, 0.0);
                mixer_mix_tracks(&mut buf, mixer_buf, tracks);
                // 如果 frame_rx 被 drop，这里的 buf 就还不回去了，MixerOutput 端会创建一个新的。
                frame_tx.send(buf);
                return true;
            }
            Ok(MixerTriggerPayload::Seek(size)) => mixer_seek_tracks(size, tracks),
            Err(RecvError) => sel.remove(trig_rx),
        }
    } else if res == *cmd_rx {
        match cmd_rx.read_select(res) {
            Ok(cmd) => mixer_handle_cmd(cmd, tracks, track_counter, events_tx, standby),
            Err(RecvError) => sel.remove(cmd_rx),
        }
    }
    false
}

/// 累加空闲计时
fn mixer_update_idle(
    read_happened: bool,
    tracks: &HashMap<TrackId, Track>,
    st: &mut StandbyState,
    events_tx: &MixerEventTx,
) {
    if !read_happened {
        return; // 只在 Read 事件后才更新。
    }
    if st.mode == StandbyMode::ForcePlay {
        st.idle_since = None;
        st.fired = false;
        return;
    }
    let now = Instant::now();
    if tracks.values().all(Track::is_idle) {
        let since = st.idle_since.get_or_insert(now);
        if !st.fired && now.duration_since(*since) >= STANDBY_DELAY {
            st.fired = true;
            // 向 device 发送待机信号
            let _ = events_tx.send(MixerEvent::RequestStandby);
            info!(idle_for = ?now.duration_since(*since), "all tracks idle, requesting standby.");
        }
    } else {
        st.idle_since = None;
        st.fired = false;
    }
}

fn mixer_mix_tracks(
    out_buf: &mut [f32],
    mixer_buf: &mut [f32],
    tracks: &mut HashMap<TrackId, Track>,
) {
    for track in tracks.values_mut() {
        track.read_frames(mixer_buf);
        // todo: 这里会爆炸，要加上削波算法
        out_buf
            .iter_mut()
            .zip(mixer_buf.iter())
            .for_each(|(o, &s)| *o += s);
    }
}

fn mixer_seek_tracks(items: usize, tracks: &mut HashMap<TrackId, Track>) {
    tracks.values_mut().for_each(|track| {
        let _ = track.skip_frames(items);
    });
}

fn mixer_handle_cmd(
    cmd: MixerCmd,
    state: &mut HashMap<TrackId, Track>,
    track_counter: &TrackIdCounter,
    events_tx: &MixerEventTx,
    standby: &mut StandbyState,
) {
    match cmd {
        MixerCmd::AddTracks(tracks, tx_oneshot) => {
            tx_oneshot.send(mixer_on_add_tracks(tracks, state, track_counter))
        }
        MixerCmd::RemoveTrack(track_ids, tx_oneshot) => {
            tx_oneshot.send(mixer_on_remove_tracks(track_ids, state))
        }
        MixerCmd::SetStandbyMode(mode, tx_oneshot) => {
            standby.mode = mode;
            standby.idle_since = None; // 重置计时
            standby.fired = false; // 恢复标志
            tx_oneshot.send(());
        }
        MixerCmd::Resume(_, tx_oneshot) => {
            // 向 device 发送唤醒信号
            let _ = events_tx.send(MixerEvent::RequestResume);
            tx_oneshot.send(());
        }
    };
}

fn mixer_on_add_tracks(
    tracks_to_add: Vec<Track>,
    state: &mut HashMap<TrackId, Track>,
    track_counter: &TrackIdCounter,
) -> Vec<TrackId> {
    tracks_to_add
        .into_iter()
        .map(|t| {
            let id = track_counter.next();
            state.insert(id, t);
            id
        })
        .collect()
}

fn mixer_on_remove_tracks(
    tracks_to_remove: Vec<TrackId>,
    state: &mut HashMap<TrackId, Track>,
) -> Vec<bool> {
    tracks_to_remove
        .iter()
        .map(|id| state.remove(id).is_some())
        .collect()
}

#[derive(Clone)]
pub struct MixerHandle {
    cmd_tx: MixerCmdTx,
}

/// MixerHandle 命令错误
#[derive(Debug, PartialEq, Eq)]
pub enum MixerCmdError {
    /// 命令通道积压或断开
    Send,
    /// mixer worker 无响应
    Timeout,
}

impl MixerHandle {
    /// 设置空闲待机模式。
    /// - Auto：空闲超时自动待机。
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

    pub fn resume(&self) -> Result<(), MixerCmdError> {
        let (tx, rx) = crossfire::oneshot::oneshot();
        self.cmd_tx
            .send_timeout(MixerCmd::Resume((), tx), Duration::from_secs(1))
            .map_err(|_| MixerCmdError::Send)?;
        rx.recv_timeout(Duration::from_secs(1))
            .map(|_| ())
            .map_err(|_| MixerCmdError::Timeout)
    }
}

#[derive(Clone)]
pub struct MixerController {
    state: Arc<MixerLinkState>,
}

impl MixerController {
    pub fn reset(&self) {
        self.state
            .read_frames_errored
            .store(false, Ordering::Relaxed);
    }

    /// 在 stream 掉线时调用
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

/// 设计上只允许一个线程读取，不要并发读。
pub struct MixerOutput {
    trig_tx: crossfire::MTx<crossfire::mpmc::Array<MixerTriggerPayload>>,
    buf: Option<Vec<f32>>,
    state: Arc<MixerLinkState>,
}

impl Clone for MixerOutput {
    fn clone(&self) -> Self {
        Self {
            trig_tx: self.trig_tx.clone(),
            buf: None,
            state: self.state.clone(),
        }
    }
}

impl MixerOutput {
    pub fn read_frames(&mut self, data: &mut [f32], timeout: Duration) -> usize {
        let mut time_left = timeout;
        if self.state.read_frames_errored.load(Ordering::Relaxed) {
            return 0; // 如果出现错误说明要么 mixer 死了，要么并发读。
        }
        // 触发 Seek 操作
        let trig_at = if self.state.disconnected.load(Ordering::Acquire) {
            // 断开的时间
            let skip_at = Instant::now();
            let dur = skip_at.duration_since(self.state.disconnect_at.load(Ordering::Acquire));
            // 2 channel * 48000 Hz * seconds
            let skip_items = 2 * (dur.as_secs_f32() * SAMPLE_RATE as f32) as usize;
            // 快进 buffer
            match self
                .trig_tx
                .send_timeout(MixerTriggerPayload::Seek(skip_items), time_left)
            {
                Err(SendTimeoutError::Timeout(_v)) => {
                    // trig channel 积攒了一个 message，刚发的 message 被退回。
                    // 只有 read_frames 被并发调用或 mixer_worker 忙时才能触发这里。但此方法不允许并发调用。
                    return 0;
                }
                Err(SendTimeoutError::Disconnected(_v)) => {
                    self.state
                        .read_frames_errored
                        .store(true, Ordering::Relaxed);
                    return 0;
                }
                _ => self.state.disconnected.store(false, Ordering::Release),
            }
            let trig_at = Instant::now();
            time_left = time_left.saturating_sub(trig_at.saturating_duration_since(skip_at));
            trig_at
        } else {
            Instant::now()
        };

        // 为 Read 操作准备循环利用的缓冲区。清空 buf 但是不影响已分配空间。
        self.buf.as_mut().map(|b| b.clear());
        // 拿出 buf，如果 buf 是None就创建新的。最坏是一次 vec 堆分配。
        let mut buf = self.buf.take().unwrap_or_default();
        // 为存放这次 stream 请求分配额外空间。
        buf.reserve(data.len().saturating_sub(buf.capacity()));

        // 触发 Read 操作。开销是一次 box 堆分配。
        let (frame_tx, frame_rx) = crossfire::oneshot::oneshot();
        match self.trig_tx.send_timeout(
            MixerTriggerPayload::Read(data.len(), buf, frame_tx),
            time_left,
        ) {
            Err(SendTimeoutError::Timeout(MixerTriggerPayload::Read(_, buf, _))) => {
                self.buf = Some(buf); // channel 积压，回收 buf。
                // 只有 read_frames 被并发调用或 mixer_worker 忙时才能触发这里。但此方法不允许并发调用。
                return 0;
            }
            Err(SendTimeoutError::Disconnected(MixerTriggerPayload::Read(_, buf, _))) => {
                self.buf = Some(buf); // channel 另一侧断开，回收 buf。
                self.state
                    .read_frames_errored
                    .store(true, Ordering::Relaxed);
                return 0; // mixer 死了
            }
            _ => (),
        }
        let recv_at = Instant::now();
        time_left = time_left.saturating_sub(recv_at.saturating_duration_since(trig_at));
        match frame_rx.recv_timeout(time_left) {
            Ok(buf) => {
                let n = buf.len().min(data.len());
                data[..n].copy_from_slice(&buf[..n]);
                self.buf = Some(buf); // 对面响应，回收 buf。
                n
            }
            Err(RecvTimeoutError::Timeout) => {
                0 // mixer_worker 响应超时。buf 会在下次调用时重新创建。
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.state
                    .read_frames_errored
                    .store(true, Ordering::Relaxed);
                0 // mixer 死了。buf 会在下次恢复时重新创建。
            }
        }
    }
}

struct MixerLinkState {
    read_frames_errored: AtomicBool,
    disconnected: AtomicBool,
    disconnect_at: AtomicInstant,
    /// unused
    _events_tx: MixerEventTx,
    events_rx: MixerEventRx,
}

/// 音频轨道
///
/// 可以放置多个不重叠的片段。
pub struct Track {
    current_clip: Option<(ClipGroup, usize)>,
    clip_queue_rx: crossfire::Rx<crossfire::mpsc::Array<ClipGroup>>,
}

impl Track {
    pub fn new() -> (Self, TrackHandle) {
        let current_clip = None;
        let (clip_queue_tx, clip_queue_rx) = crossfire::mpsc::bounded_async_blocking(128);
        (
            Self {
                current_clip,
                clip_queue_rx,
            },
            TrackHandle { clip_queue_tx },
        )
    }

    pub fn skip_frames(&mut self, items: usize) -> usize {
        let mut remaining = items;
        while remaining > 0 {
            if self.current_clip.is_none() && !self.try_fetch_next_clip() {
                break;
            }
            if let Some((clip, pos)) = self.current_clip.as_mut() {
                let n = clip.skip_items(remaining);
                *pos += n;
                remaining -= n;
                if n == 0 {
                    self.current_clip = None;
                }
            }
        }
        items - remaining
    }

    /// 读取音频轨道，填满 data。内容不足的部分补 0。
    pub fn read_frames(&mut self, data: &mut [f32]) {
        let mut written = 0;
        while written < data.len() {
            if self.current_clip.is_none() && !self.try_fetch_next_clip() {
                break;
            }
            let (clip, pos) = self.current_clip.as_mut().expect("fetched above");
            let n = clip.read_frames(*pos, &mut data[written..]);
            *pos += n;
            written += n;
            if clip.is_empty() {
                self.current_clip = None;
            }
        }
        data[written..].fill(0.0);
    }

    /// 轨道是否空闲
    pub fn is_idle(&self) -> bool {
        let current_done = self
            .current_clip
            .as_ref()
            .map_or(true, |(g, _)| g.is_empty());
        current_done && self.clip_queue_rx.is_empty()
    }

    /// 尝试从 Rx 队列中拉取下一个可用的 Clip
    fn try_fetch_next_clip(&mut self) -> bool {
        while let Ok(clip) = self.clip_queue_rx.try_recv() {
            if let Some(c) = clip.into_current_clip() {
                self.current_clip = Some(c);
                return true;
            }
        }
        false
    }
}

pub struct TrackHandle {
    clip_queue_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<ClipGroup>>,
}

impl TrackHandle {}

pub struct Clip {
    /// Clip 的音频数据
    sample: Vec<f32>,
    /// true = 无论如何都跳过播放这个 Clip
    skip: Arc<AtomicBool>,
    /// true = 只在从 Track 队列取出时跳过播放这个 Clip
    timeout: Arc<AtomicBool>,
    /// Clip 播放完 drop 信号
    _done_tx: crossfire::null::CloseHandle<crossfire::mpsc::Null>,
}

impl Clip {
    pub fn new(sample: Vec<f32>) -> (Self, ClipHandle) {
        let skip: Arc<AtomicBool> = Default::default();
        let timeout: Arc<AtomicBool> = Default::default();
        let (done_tx, done_rx): (
            crossfire::null::CloseHandle<crossfire::mpsc::Null>,
            crossfire::AsyncRx<crossfire::mpsc::Null>,
        ) = crossfire::mpsc::Null::new().new_async();
        (
            Self {
                sample,
                skip: skip.clone(),
                timeout: timeout.clone(),
                _done_tx: done_tx,
            },
            ClipHandle {
                done_rx,
                skip,
                timeout,
            },
        )
    }

    /// 从 Clip 的第 start_idx 个元素开始向后填充 data。返回成功填充的元素数。
    /// - `元素数 * 通道数 = 采样数`
    /// - 直接用新值覆盖 data，不会碰未填充部分。
    pub fn read_frames(&self, start_idx: usize, data: &mut [f32]) -> usize {
        if self.skip.load(Ordering::Relaxed) {
            return 0;
        }
        if start_idx >= self.sample.len() {
            return 0;
        }
        let available = &self.sample[start_idx..];
        let count = available.len().min(data.len());
        data[..count].copy_from_slice(&available[..count]);
        count
    }

    pub(crate) fn size(&self) -> usize {
        if self.skip.load(Ordering::Relaxed) {
            return 0;
        }
        self.sample.len()
    }

    pub(crate) fn timed_out(&self) -> bool {
        self.timeout.load(Ordering::Relaxed)
    }
}

pub struct ClipHandle {
    skip: Arc<AtomicBool>,
    timeout: Arc<AtomicBool>,
    done_rx: crossfire::AsyncRx<crossfire::mpsc::Null>,
}

pub struct ClipGroup {
    /// Clip 队列，第一个是正在播放的
    clips: VecDeque<Clip>,
    /// 正在播放的 Clip 进度
    clip_pos: usize,
    /// ClipGroup 进度
    pos: usize,
    skip: Arc<AtomicBool>,
    timeout: Arc<AtomicBool>,
    _done_tx: crossfire::null::CloseHandle<crossfire::mpsc::Null>,
}

impl ClipGroup {
    pub fn new(clips: Vec<Clip>) -> (Self, ClipGroupHandle) {
        debug_assert!(!clips.is_empty(), "ClipGroup can't be empty");
        let skip: Arc<AtomicBool> = Default::default();
        let timeout: Arc<AtomicBool> = Default::default();
        let (done_tx, done_rx): (
            crossfire::null::CloseHandle<crossfire::mpsc::Null>,
            crossfire::AsyncRx<crossfire::mpsc::Null>,
        ) = crossfire::mpsc::Null::new().new_async();
        (
            Self {
                clips: clips.into(),
                clip_pos: 0,
                pos: 0,
                skip: skip.clone(),
                timeout: timeout.clone(),
                _done_tx: done_tx,
            },
            ClipGroupHandle {
                done_rx,
                skip,
                timeout,
            },
        )
    }

    pub fn skip_items(&mut self, mut items: usize) -> usize {
        let mut skipped = 0;
        while items > 0 {
            while self.clips.front().map_or(false, |c| c.timed_out()) {
                self.clips.pop_front();
                self.clip_pos = 0;
            }
            let Some(clip) = self.clips.front_mut() else {
                break;
            };
            let clip_remaining = clip.size() - self.clip_pos;
            if clip_remaining == 0 {
                self.clips.pop_front();
                self.clip_pos = 0;
                continue;
            }
            if items >= clip_remaining {
                items -= clip_remaining;
                skipped += clip_remaining;
                self.clips.pop_front();
                self.clip_pos = 0;
            } else {
                self.clip_pos += items;
                skipped += items;
                items = 0;
            }
        }
        self.pos += skipped;
        skipped
    }

    pub fn read_frames(&mut self, start_idx: usize, data: &mut [f32]) -> usize {
        if self.skip.load(Ordering::Relaxed) {
            return 0;
        }
        debug_assert_eq!(start_idx, self.pos, "read backward is unsupported");
        let mut written = 0; // 本次读出的数据量
        while written < data.len() {
            while self.clips.front().map_or(false, |c| c.timed_out()) {
                self.clips.pop_front(); // 跳过已超时的 Clip
                self.clip_pos = 0;
            }
            let Some(clip) = self.clips.front_mut() else {
                break; // 队列里没有 Clip 了
            };
            // 读取找到的 Clip
            let n = clip.read_frames(self.clip_pos, &mut data[written..]);
            if n == 0 {
                self.clips.pop_front(); // Clip 读不出东西
                self.clip_pos = 0;
            } else {
                self.clip_pos += n; // 更新进度
                written += n;
            }
        }
        self.pos += written; // 更新进度
        written
    }

    /// ClipGroup 内是否已没有可播放的 Clip
    pub(crate) fn is_empty(&self) -> bool {
        self.clips.is_empty()
    }

    pub fn into_current_clip(self) -> Option<(Self, usize)> {
        if self.timeout.load(Ordering::Relaxed) {
            None
        } else {
            Some((self, 0))
        }
    }
}

pub struct ClipGroupHandle {
    skip: Arc<AtomicBool>,
    timeout: Arc<AtomicBool>,
    done_rx: crossfire::AsyncRx<crossfire::mpsc::Null>,
}

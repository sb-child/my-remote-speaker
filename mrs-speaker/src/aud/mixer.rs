use crate::aud::SAMPLE_RATE;
use crossfire::{BlockingTxTrait, RecvTimeoutError, SendTimeoutError};
use my_remote_speaker::{
    task::{TaskHandle, TaskManager},
    util::AtomicInstant,
};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// 混音器管理器
///
/// 可以动态创建和删除混音器。
pub struct Mixers {}

pub enum MixerCmd {
    AddTrack((Track, crossfire::Rx<crossfire::spsc::Array<u64>>)),
    RemoveTrack((u64, crossfire::Rx<crossfire::spsc::Array<bool>>)),
    InsertClip((u64, ClipGroup, crossfire::Rx<crossfire::spsc::Array<bool>>)),
}

type MixerTrigRx = crossfire::MRx<
    crossfire::mpmc::Array<(usize, Option<crossfire::oneshot::TxOneshot<Vec<f32>>>)>,
>;

/// 混音器
///
/// 可以挂载多个轨道，合并为一个输出。
pub struct Mixer {
    trig_rx: MixerTrigRx,
    worker: Option<TaskHandle<(), (), ()>>,
}

impl Mixer {
    pub fn new(tm: &TaskManager) -> (Self, MixerOutput) {
        let (trig_tx, trig_rx) = crossfire::mpmc::bounded_blocking(1);
        let worker = spawn_mixer_worker(tm, trig_rx.clone());
        (
            Self {
                trig_rx,
                worker: Some(worker),
            },
            MixerOutput {
                trig_tx,
                read_frames_errored: Default::default(),
                disconnected: Default::default(),
                disconnect_at: AtomicInstant::new(Instant::now()).into(),
            },
        )
    }

    pub fn restart(&mut self, tm: &TaskManager) {
        if let Some(w) = self.worker.take() {
            w.cancel(); // worker 不会立刻关闭
            loop {
                thread::sleep(Duration::from_millis(100));
                if let Some(ts) = w.status() {
                    if ts.is_terminal() {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        self.worker = Some(spawn_mixer_worker(tm, self.trig_rx.clone()));
    }
}

fn spawn_mixer_worker(tm: &TaskManager, trig_rx: MixerTrigRx) -> TaskHandle<(), (), ()> {
    tm.spawn_blocking_typed(move |pc, ct| {
        pc.update(()).ok();
        mixer_worker(trig_rx, ct);
        Ok(())
    })
}

fn mixer_worker(trig_rx: MixerTrigRx, ct: CancellationToken) {
    loop {
        let (size, r) = match trig_rx.recv_timeout(Duration::from_millis(50)) {
            Ok(x) => x,
            Err(RecvTimeoutError::Timeout) => {
                if ct.is_cancelled() {
                    return;
                }
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => {
                return;
            }
        };
        if let Some(frame_tx) = r {
            let buf = mix_tracks(size);
            frame_tx.send(buf); // 如果 frame_rx 被 drop，这里发送的 buf 会自动 drop。
        } else {
            seek_tracks(size);
        }
    }
}

fn mix_tracks(items: usize) -> Vec<f32> {
    let buf = Vec::new();
    // todo: mix tracks
    buf
}

fn seek_tracks(items: usize) {
    // todo
}

pub struct MixerHandle {}

/// 设计上只允许一个线程读取，不要并发读。
#[derive(Clone)]
pub struct MixerOutput {
    trig_tx: crossfire::MTx<
        crossfire::mpmc::Array<(usize, Option<crossfire::oneshot::TxOneshot<Vec<f32>>>)>,
    >,
    read_frames_errored: Arc<AtomicBool>,
    disconnected: Arc<AtomicBool>,
    disconnect_at: Arc<AtomicInstant>,
}

impl MixerOutput {
    pub fn reset(&self) {
        self.read_frames_errored.store(false, Ordering::Relaxed);
    }

    /// 在 stream 掉线时调用
    pub fn disconnected(&self) {
        if self.disconnected.swap(true, Ordering::Release) {
            return; // 只记录第一次断开连接时间
        }
        self.disconnect_at.store(Instant::now(), Ordering::Release);
    }

    pub fn read_frames(&self, data: &mut [f32], timeout: Duration) -> usize {
        let mut time_left = timeout;
        // let mut start_at = Instant::now();
        if self.read_frames_errored.load(Ordering::Relaxed) {
            return 0; // 如果出现错误说明要么 mixer 死了，要么并发读。
        }
        let trig_at = if self.disconnected.load(Ordering::Acquire) {
            // 断开的时间
            let skip_at = Instant::now();
            let dur = skip_at.duration_since(self.disconnect_at.load(Ordering::Acquire));
            // 2 channel * 48000 Hz * seconds
            let skip_items = 2 * (dur.as_secs_f32() * SAMPLE_RATE as f32) as usize;
            // 快进 buffer
            match self.trig_tx.send_timeout((skip_items, None), time_left) {
                Err(SendTimeoutError::Timeout(_v)) => {
                    // trig channel 积攒了一个 message，刚发的 message 被退回。
                    // 只有 read_frames 被并发调用或 mixer_worker 忙时才能触发这里。但此方法不允许并发调用。
                    return 0;
                }
                Err(SendTimeoutError::Disconnected(_v)) => {
                    self.read_frames_errored.store(true, Ordering::Relaxed);
                    return 0;
                }
                _ => self.disconnected.store(false, Ordering::Release),
            }
            let trig_at = Instant::now();
            time_left = time_left.saturating_sub(trig_at.saturating_duration_since(skip_at));
            trig_at
        } else {
            Instant::now()
        };
        // 开销是一次box堆分配
        let (frame_tx, frame_rx) = crossfire::oneshot::oneshot();
        match self
            .trig_tx
            .send_timeout((data.len(), Some(frame_tx)), time_left)
        {
            Err(SendTimeoutError::Timeout(_v)) => {
                // trig channel 积攒了一个 message，刚发的 message 被退回。
                // 上一个 `Some(frame_tx)` 的 `frame_rx` 已被 drop，让 worker 对它发送的内容被自动 drop 所以没问题
                // 只有 read_frames 被并发调用或 mixer_worker 忙时才能触发这里。但此方法不允许并发调用。
                return 0;
            }
            Err(SendTimeoutError::Disconnected(_v)) => {
                self.read_frames_errored.store(true, Ordering::Relaxed);
                return 0; // mixer 死了
            }
            _ => (),
        }
        let recv_at = Instant::now();
        time_left = time_left.saturating_sub(recv_at.saturating_duration_since(trig_at));
        match frame_rx.recv_timeout(time_left) {
            Ok(x) => {
                let n = x.len().min(data.len());
                data[..n].copy_from_slice(&x[..n]);
                n
            }
            Err(RecvTimeoutError::Timeout) => {
                0 // mixer_worker 响应超时
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.read_frames_errored.store(true, Ordering::Relaxed);
                0 // mixer 死了
            }
        }
    }
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

    /// 读取音频轨道，填充 data。返回成功填充的元素数。
    pub fn read_frames(&mut self, mut data: &mut [f32]) -> usize {
        let total_requested = data.len();
        while !data.is_empty() {
            if self.current_clip.is_none() && !self.try_fetch_next_clip() {
                break;
            }
            if let Some((clip, pos)) = self.current_clip.as_mut() {
                let n = clip.read_frames(*pos, data);
                if n == 0 {
                    self.current_clip = None;
                } else {
                    *pos += n;
                    data = &mut data[n..];
                }
            }
        }

        total_requested - data.len()
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

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// 混音器管理器
///
/// 可以动态创建和删除混音器。
pub struct Mixers {}

pub enum MixerCmd {
    AddTrack((Track, crossfire::Rx<crossfire::spsc::Array<u64>>)),
    RemoveTrack((u64, crossfire::Rx<crossfire::spsc::Array<bool>>)),
    InsertClip((u64, ClipGroup, crossfire::Rx<crossfire::spsc::Array<bool>>)),
}

/// 混音器
///
/// 可以挂载多个轨道，合并为一个输出。
pub struct Mixer {
    trig_rx:
        crossfire::MRx<crossfire::mpmc::Array<(usize, crossfire::oneshot::TxOneshot<Vec<f32>>)>>,
}

impl Mixer {
    pub fn new() -> (Self, MixerOutput) {
        let (trig_tx, trig_rx) = crossfire::mpmc::bounded_blocking(1);
        let out_errored = Default::default();
        (
            Self { trig_rx },
            MixerOutput {
                trig_tx,
                errored: out_errored,
            },
        )
    }
}

fn mixer_thread() {}

pub struct MixerHandle {}

/// 设计上只允许一个线程读取，不要并发读。
#[derive(Clone)]
pub struct MixerOutput {
    trig_tx:
        crossfire::MTx<crossfire::mpmc::Array<(usize, crossfire::oneshot::TxOneshot<Vec<f32>>)>>,
    errored: Arc<AtomicBool>,
}

impl MixerOutput {
    pub fn reset(&self) {
        self.errored.store(false, Ordering::Relaxed);
    }

    pub fn read_frames(&self, data: &mut [f32], timeout: Duration) -> usize {
        if self.errored.load(Ordering::Relaxed) {
            return 0; // 如果出现错误说明要么 mixer_thread 死了，要么并发读。
        }
        // 开销是一次box堆分配
        let (frame_tx, frame_rx) = crossfire::oneshot::oneshot();
        let request_instant = Instant::now();
        match self.trig_tx.send_timeout((data.len(), frame_tx), timeout) {
            Err(SendTimeoutError::Timeout(_v)) => {
                // 因为 mixer_thread 超时，trig_tx 积攒了一个 message。
                // mixer_thread 最终会发现 frame_rx 已被 drop 所以没问题。
                return 0;
            }
            Err(SendTimeoutError::Disconnected(_v)) => {
                self.errored.store(true, Ordering::Relaxed);
                return 0; // mixer_thread 死了
            }
            _ => (),
        }
        let response_timeout =
            timeout.saturating_sub(Instant::now().duration_since(request_instant));
        match frame_rx.recv_timeout(response_timeout) {
            Ok(x) => {
                let n = x.len().min(data.len());
                data[..n].copy_from_slice(&x[..n]);
                n
            }
            Err(RecvTimeoutError::Timeout) => {
                0 // mixer_thread 响应超时
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.errored.store(true, Ordering::Relaxed);
                0 // mixer_thread 死了
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

    pub(crate) fn is_timeout(&self) -> bool {
        self.timeout.load(Ordering::Relaxed)
    }
}

pub struct ClipHandle {
    skip: Arc<AtomicBool>,
    timeout: Arc<AtomicBool>,
    done_rx: crossfire::AsyncRx<crossfire::mpsc::Null>,
}

use std::collections::VecDeque;

use crossfire::{BlockingTxTrait, RecvTimeoutError, SendTimeoutError};

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

    pub fn read_frames(&mut self, start_idx: usize, data: &mut [f32]) -> usize {
        if self.skip.load(Ordering::Relaxed) {
            return 0;
        }
        debug_assert_eq!(start_idx, self.pos, "read backward is unsupported");
        let mut written = 0; // 本次读出的数据量
        while written < data.len() {
            while self.clips.front().map_or(false, |c| c.is_timeout()) {
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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// 混音器管理器
///
/// 可以动态创建和删除混音器。
pub struct Mixers {}

pub enum MixerCmd {
    AddTrack((Track, crossfire::Rx<crossfire::spsc::Array<u64>>)),
    RemoveTrack((u64, crossfire::Rx<crossfire::spsc::Array<bool>>)),
    InsertClip((u64, Clip, crossfire::Rx<crossfire::spsc::Array<bool>>)),
}

/// 混音器
///
/// 可以挂载多个轨道，合并为一个输出。
pub struct Mixer {
    output_tx: crossfire::MTx<crossfire::mpmc::Array<Vec<f32>>>,
    trig_rx: crossfire::MRx<crossfire::mpmc::Array<usize>>,
}

impl Mixer {
    pub fn new() -> (Self, MixerOutput) {
        let (output_tx, output_rx) = crossfire::mpmc::bounded_blocking(1);
        let (trig_tx, trig_rx) = crossfire::mpmc::bounded_blocking(1);
        (
            Self { output_tx, trig_rx },
            MixerOutput { output_rx, trig_tx },
        )
    }
}

fn mixer_thread() {}

pub struct MixerHandle {}

/// 设计上只允许一个线程读取，不要并发读。
#[derive(Clone)]
pub struct MixerOutput {
    output_rx: crossfire::MRx<crossfire::mpmc::Array<Vec<f32>>>,
    trig_tx: crossfire::MTx<crossfire::mpmc::Array<usize>>,
}

impl MixerOutput {
    pub fn read_frames(&self, data: &mut [f32]) -> usize {
        if let Err(_e) = self.trig_tx.send(data.len()) {
            return 0; // 不应该
        }
        match self.output_rx.recv() {
            Ok(x) => {
                data.copy_from_slice(&x); // Mixer 应该负责
                x.len()
            }
            Err(_e) => {
                0 // 不应该
            }
        }
    }
}

/// 音频轨道
///
/// 可以放置多个不重叠的片段。
pub struct Track {
    current_clip: Option<(Clip, usize)>,
    clip_queue_rx: crossfire::Rx<crossfire::mpsc::Array<Clip>>,
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
    clip_queue_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<Clip>>,
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

    pub fn into_current_clip(self) -> Option<(Self, usize)> {
        if self.timeout.load(Ordering::Relaxed) {
            None
        } else {
            Some((self, 0))
        }
    }
}

pub struct ClipHandle {
    skip: Arc<AtomicBool>,
    timeout: Arc<AtomicBool>,
    done_rx: crossfire::AsyncRx<crossfire::mpsc::Null>,
}

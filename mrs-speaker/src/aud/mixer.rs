use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// 混音器管理器
///
/// 可以动态创建和删除混音器。
pub struct Mixers {}

/// 混音器
///
/// 可以挂载多个轨道，合并为一个输出。
pub struct Mixer {}

/// 音频轨道
///
/// 可以放置多个不重叠的片段。
pub struct Track {
    current_clip: Option<(Clip, usize)>,
    clip_queue: crossfire::Rx<crossfire::mpsc::Array<()>>,
}

impl Track {
    pub fn read_frames(&self, data: &mut [f32]) {
        ()
    }
}

pub struct TrackHandle {}

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
    ///
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

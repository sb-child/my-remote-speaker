pub mod handler;
pub mod mixer;
pub mod scheduler;

use my_remote_speaker::task::TaskManager;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use crate::aud::{handler::host_handler, mixer::Mixers};

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

pub struct AudioManager {}

impl AudioManager {
    pub async fn new(tm: TaskManager, ct: CancellationToken) -> Self {
        let mixers = Arc::new(Mixers::new(tm.clone(), ct.clone()));
        let mixers_for_host_handler = mixers.clone();
        let h = tm.spawn_blocking_typed(move |tm, pu, ct| {
            host_handler(tm, pu, ct, mixers_for_host_handler);
            Ok::<(), ()>(())
        });
        h.cancel_at(&ct);
        h.wait_for(|s| s.is_running()).await;
        Self {}
    }
}

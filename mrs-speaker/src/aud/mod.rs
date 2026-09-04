pub mod handler;
pub mod mixer;
pub mod scheduler;

use crate::aud::{
    handler::{HostHandlerError, host_handler},
    mixer::Mixers,
};
use my_remote_speaker::task::{TaskManager, TypedTaskState};
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

pub struct AudioManager {
    _guard: DropGuard,
}

impl AudioManager {
    pub async fn new(tm: TaskManager, ct: CancellationToken) -> Result<Self, AudioManagerError> {
        let mixers = Arc::new(Mixers::new(tm.clone(), ct.clone()));
        let mixers_for_host_handler = mixers.clone();
        let h = tm.spawn_blocking_typed(move |tm, pu, ct| {
            host_handler(tm, pu, ct, mixers_for_host_handler)?;
            Ok::<(), HostHandlerError>(())
        });
        h.cancel_at(&ct);
        let s = h.wait_for(|s| s.is_running()).await;
        ensure!(
            matches!(
                s,
                TypedTaskState::Cancelled
                    | TypedTaskState::Cancelling
                    | TypedTaskState::Completed(_)
            ),
            HostHandlerExitedSnafu
        );
        if !s.is_running() {
            // todo
            match s {
                TypedTaskState::Cancelled | TypedTaskState::Cancelling => {}
                TypedTaskState::Completed(_) => {}
                TypedTaskState::Failed(_) => todo!(),
                TypedTaskState::Cancelled => todo!(),
                TypedTaskState::Panicked(_) => todo!(),
                _ => {
                    unreachable!()
                }
            }
        }

        Ok(Self {
            _guard: ct.drop_guard(),
        })
    }
}

#[derive(Snafu, Debug)]
pub enum AudioManagerError {
    HostHandler { source: HostHandlerError },
    HostHandlerExited,
    HostHandlerPanicked { msg: String },
}

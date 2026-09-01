pub mod dcblocker;
pub mod handler;
pub mod mixer;
pub mod scheduler;

use my_remote_speaker::task::TaskManager;

/// Sample Rate = 48000 Hz
pub const SAMPLE_RATE: u32 = 48000;

pub struct AudioManager {}

impl AudioManager {
    pub fn new(tm: TaskManager) -> Self {
        Self {}
    }
}

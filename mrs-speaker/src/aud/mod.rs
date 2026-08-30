pub mod dcblocker;
pub mod handler;
pub mod mixer;
pub mod scheduler;

use my_remote_speaker::task::TaskManager;

pub struct AudioManager {}

impl AudioManager {
    pub fn new(tm: TaskManager) -> Self {
        Self {}
    }
}

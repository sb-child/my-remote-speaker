use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::aud::SAMPLE_RATE;

#[derive(Debug, Clone)]
pub struct DcBlocker {
    r: f32,
    x_prev: [f32; 2],
    y_prev: [f32; 2],
    reset_requested: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct DcBlockerHandle {
    reset_requested: Arc<AtomicBool>,
}

impl DcBlockerHandle {
    pub fn reset(&self) {
        self.reset_requested.store(true, Ordering::Release);
    }
}

impl DcBlocker {
    pub fn new(cutoff_hz: f32, sample_rate: f32) -> (Self, DcBlockerHandle) {
        let r = 1.0 - ((2.0 * std::f32::consts::PI * cutoff_hz) / sample_rate);
        let r = r.clamp(0.90, 0.9999);
        let reset_requested = Arc::new(AtomicBool::new(false));
        let handle = DcBlockerHandle {
            reset_requested: reset_requested.clone(),
        };
        (
            Self {
                r,
                x_prev: [0.0; 2],
                y_prev: [0.0; 2],
                reset_requested,
            },
            handle,
        )
    }

    pub fn default_48k() -> (Self, DcBlockerHandle) {
        Self::new(10.0, SAMPLE_RATE as f32)
    }

    #[inline(always)]
    pub fn process_interleaved(&mut self, data: &mut [f32]) {
        if self.reset_requested.swap(false, Ordering::Acquire) {
            self.x_prev = [0.0; 2];
            self.y_prev = [0.0; 2];
        }
        let r = self.r;
        for chunk in data.chunks_exact_mut(2) {
            let x_l = chunk[0];
            let y_l = x_l - self.x_prev[0] + r * self.y_prev[0];
            self.x_prev[0] = x_l;
            self.y_prev[0] = if y_l.abs() < 1e-15 { 0.0 } else { y_l };
            chunk[0] = self.y_prev[0];
            let x_r = chunk[1];
            let y_r = x_r - self.x_prev[1] + r * self.y_prev[1];
            self.x_prev[1] = x_r;
            self.y_prev[1] = if y_r.abs() < 1e-15 { 0.0 } else { y_r };
            chunk[1] = self.y_prev[1];
        }
    }
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Channels {
    Mono,
    Stereo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sample {
    sample_rate: u32,
    channels: Channels,
    #[serde(with = "serde_bytes")]
    raw_data: Vec<u8>,
}

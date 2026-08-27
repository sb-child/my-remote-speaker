use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct KeypairConf {
    #[serde(with = "serde_bytes")]
    pub public_part: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub private_part: [u8; 32],
}

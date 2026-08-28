use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct LibLaunchArgs {
    pub launch_mode: LaunchMode,
    pub conf_path: PathBuf,
    pub temp_path: PathBuf,
    pub stop_file: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum LaunchMode {
    Magisk {
        mod_id: String,
        module_path: PathBuf,
    },
    Normal,
}

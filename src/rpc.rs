use std::time::Duration;

use iroh_blobs::{Hash, ticket::BlobTicket};

use crate::task::TaskId;

#[tarpc::service]
pub trait MrsRpcTrait {
    /// 存储音频
    async fn store_audio_sample(sample_ticket: BlobTicket) -> TaskId;
    /// 获取存储音频进度
    async fn store_audio_sample_task_state(
        tid: TaskId,
    ) -> Result<StoreSampleTaskState, TaskManageError>;
    /// 查询音频信息
    async fn query_audio_sample(sample_hash: Hash) -> Result<SampleInfo, QuerySampleError>;
    /// 删除音频
    async fn remove_audio_sample(sample_hash: Hash) -> Result<(), RemoveSampleError>;
}

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
pub enum TaskManageError {
    #[error("Task not found.")]
    NotFound,
    #[error("Task downcast error.")]
    DowncastError,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StoreSampleTaskState {
    Pending,
    Running(StoreSampleProgress),
    Completed(()),
    Failed(StoreSampleError),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StoreSampleProgress {
    CheckingDatabase,
    DownloadingSample,
    ReadingBlob,
    CheckingSampleData,
    CommittingDatabase,
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StoreSampleError {
    #[error("Ticket can not be reached: {0}")]
    TicketNotReached(String),
    #[error("Database Error: {0}")]
    DatabaseError(String),
    #[error("Audio sample exists.")]
    SampleExists,
    #[error("Audio sample is broken.")]
    BrokenSample,
    #[error("Data pack is broken.")]
    BrokenPack,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SampleInfo {
    length: Duration,
    rate: u32,
}

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
pub enum QuerySampleError {
    #[error("Hash not found.")]
    NotFound,
}

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
pub enum RemoveSampleError {
    #[error("Hash not found.")]
    NotFound,
}

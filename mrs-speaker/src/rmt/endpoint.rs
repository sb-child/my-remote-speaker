use crate::rmt::State;
use futures::StreamExt as _;
use iroh::Endpoint;
use iroh_blobs::{Hash, store::mem::MemStore, ticket::BlobTicket};
use my_remote_speaker::{
    rpc::{
        MrsRpcTrait, QuerySampleError, RemoveSampleError, SampleInfo, StoreSampleError,
        StoreSampleProgress, StoreSampleTaskState, TaskManageError,
    },
    task::{ProgressUpdater, TaskId, TypedTaskState},
};
use surrealkv::Tree;
use tarpc::{
    server::Channel as _, tokio_serde::formats::Bincode, tokio_util::codec::LengthDelimitedCodec,
};
use tokio::io::AsyncReadExt as _;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct RpcEp {
    pub ctx: State,
}

impl iroh::protocol::ProtocolHandler for RpcEp {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        // thanks https://github.com/TotalKrill/iroh-tarpc/blob/main/src/main.rs
        let rpc = self.clone();
        let conn = connection.accept_bi().await?;
        let stream = tokio::io::join(conn.1, conn.0);
        let framed = LengthDelimitedCodec::builder().new_framed(stream);
        let server_transport = tarpc::serde_transport::new(framed, Bincode::default());
        let server = tarpc::server::BaseChannel::with_defaults(server_transport);
        Box::pin(async move {
            server
                .execute(rpc.serve())
                .for_each(|response| async move {
                    tokio::spawn(response);
                })
                .await;
            Ok(())
        })
        .await
    }
}

async fn store_audio_sample_task(
    ticket: BlobTicket,
    ep: Endpoint,
    ss: Tree,
    stat: ProgressUpdater<StoreSampleProgress>,
    _ct: CancellationToken,
) -> Result<(), StoreSampleError> {
    stat.update(StoreSampleProgress::CheckingDatabase);
    let mut tx = ss
        .begin()
        .map_err(|e| StoreSampleError::DatabaseError(e.to_string()))?;
    let hash_bytes = ticket.hash().as_bytes().to_vec();
    let db_result = tx
        .get(&hash_bytes)
        .map_err(|e| StoreSampleError::DatabaseError(e.to_string()))?;
    if let Some(_) = db_result {
        return Err(StoreSampleError::SampleExists);
    }
    stat.update(StoreSampleProgress::DownloadingSample);
    let memstore = MemStore::new();
    let downloader = memstore.downloader(&ep);
    downloader
        .download(ticket.hash(), Some(ticket.addr().id))
        .await
        .map_err(|e| StoreSampleError::TicketNotReached(e.to_string()))?;
    stat.update(StoreSampleProgress::ReadingBlob);
    // should not be an error
    let mut reader = memstore.blobs().reader(ticket.hash());
    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .await
        .map_err(|e| StoreSampleError::TicketNotReached(e.to_string()))?;
    stat.update(StoreSampleProgress::CheckingSampleData);
    // todo: 检查sample数据
    stat.update(StoreSampleProgress::CommittingDatabase);
    tx.set(&hash_bytes, &buffer)
        .map_err(|e| StoreSampleError::DatabaseError(e.to_string()))?;
    tx.commit()
        .await
        .map_err(|e| StoreSampleError::DatabaseError(e.to_string()))?;
    Ok(())
}

impl MrsRpcTrait for RpcEp {
    async fn store_audio_sample(
        self,
        _context: ::tarpc::context::Context,
        sample_ticket: BlobTicket,
    ) -> TaskId {
        let ep = self.ctx.endpoint;
        let ss = self.ctx.sample_store;
        let tid = self
            .ctx
            .task_manager
            .clone()
            .spawn(|stat, ct| store_audio_sample_task(sample_ticket, ep, ss, stat, ct));
        tid
    }

    async fn store_audio_sample_task_state(
        self,
        _context: ::tarpc::context::Context,
        tid: TaskId,
    ) -> Result<StoreSampleTaskState, TaskManageError> {
        if let Some(Some(state)) =
            self.ctx.task_manager.get_status(tid).map(|x| {
                x.to_typed::<StoreSampleProgress, StoreSampleTaskState, StoreSampleError>()
            })
        {
            match state {
                TypedTaskState::Pending => Ok(StoreSampleTaskState::Pending),
                TypedTaskState::Running(progress) => {
                    Ok(StoreSampleTaskState::Running(progress.as_ref().clone()))
                }
                TypedTaskState::Completed(completed) => Ok(completed.as_ref().clone()),
                TypedTaskState::Failed(err) => {
                    Ok(StoreSampleTaskState::Failed(err.as_ref().clone()))
                }
                TypedTaskState::Panicked(_)
                | TypedTaskState::Cancelling
                | TypedTaskState::Cancelled
                | TypedTaskState::Invalid => Err(TaskManageError::NotFound),
            }
        } else {
            Err(TaskManageError::NotFound)
        }
    }

    async fn query_audio_sample(
        self,
        _context: ::tarpc::context::Context,
        _sample_hash: Hash,
    ) -> Result<SampleInfo, QuerySampleError> {
        todo!()
    }

    async fn remove_audio_sample(
        self,
        _context: ::tarpc::context::Context,
        _sample_hash: Hash,
    ) -> Result<(), RemoveSampleError> {
        todo!()
    }
}

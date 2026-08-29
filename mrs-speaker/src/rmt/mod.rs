use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_mainline_address_lookup::DhtAddressLookup;
use iroh_mdns_address_lookup::MdnsAddressLookup;
use my_remote_speaker::task::TaskManager;
use std::fmt::Debug;
use surrealkv::Tree;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::rmt::{
    endpoint::RpcEp, keypair::KeypairService, sample_cache::SampleCacheService,
    sample_store::SampleStoreService,
};

pub mod endpoint;
pub mod keypair;
pub mod sample_cache;
pub mod sample_store;

pub async fn bind_endpoint(
    kps: KeypairService,
    smps: SampleStoreService,
    smcs: SampleCacheService,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let secret_key = tokio::task::spawn_blocking(move || kps.read_secret_key()).await??;
    let sample_store = smps.open().await?;
    let sample_cache = smcs.open().await?;
    let task_manager = TaskManager::new();
    let ep = iroh::Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .address_lookup(DhtAddressLookup::builder())
        .address_lookup(MdnsAddressLookup::builder())
        .bind()
        .await?;
    let ctx = State::new(
        sample_store.clone(),
        sample_cache.clone(),
        ep.clone(),
        task_manager.clone(),
    );
    let mrs_rpc_alpn = b"mrs/s/rpc";
    let router = Router::builder(ep.clone())
        .accept(mrs_rpc_alpn, RpcEp { ctx: ctx.clone() })
        .spawn();
    tokio::select! {
        _ = cancel_token.cancelled() => stop_all_things(router, ep, sample_store, sample_cache, task_manager).await
    }
    Ok(())
}

async fn stop_all_things(
    router: Router,
    ep: Endpoint,
    sample_store: Tree,
    sample_cache: Tree,
    task_manager: TaskManager,
) {
    info!("Stopping bind_endpoint task");
    if let Err(e) = router.shutdown().await {
        warn!("Error while stopping iroh router: {}", e);
    } else {
        info!("Stopped iroh router");
    }
    ep.close().await;
    info!("Stopped iroh endpoint");
    if let Err(e) = sample_store.close().await {
        warn!("Error while stopping sample_store database: {}", e);
    } else {
        info!("Stopped sample_store database");
    }
    if let Err(e) = sample_cache.close().await {
        warn!("Error while stopping sample_cache database: {}", e);
    } else {
        info!("Stopped sample_cache database");
    }
    task_manager.close();
    info!("Stopped task_manager database");
}

#[derive(Clone)]
pub struct State {
    pub sample_store: Tree,
    pub sample_cache: Tree,
    pub endpoint: Endpoint,
    pub task_manager: TaskManager,
}

impl State {
    pub fn new(
        sample_store: Tree,
        sample_cache: Tree,
        endpoint: Endpoint,
        task_manager: TaskManager,
    ) -> Self {
        Self {
            sample_store,
            sample_cache,
            endpoint,
            task_manager,
        }
    }

    pub fn get_sample_store(&self) -> Tree {
        self.sample_store.clone()
    }

    pub fn get_sample_cache(&self) -> Tree {
        self.sample_cache.clone()
    }

    pub fn get_endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    pub fn get_task_manager(&self) -> TaskManager {
        self.task_manager.clone()
    }
}

impl Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State").finish()
    }
}

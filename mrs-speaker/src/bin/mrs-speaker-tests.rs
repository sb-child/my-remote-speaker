use clap::{Parser, Subcommand};
use mrs_speaker::aud;
use my_remote_speaker::task::TaskManager;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

/// mrs-speaker Test CLI
#[derive(Parser, Debug)]
#[command(
    name = "mrs-speaker",
    author,
    version,
    about = "mrs-speaker Test CLI",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

/// sub-commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    Audio,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,my_remote_speaker=info,mrs_speaker=info"));
    fmt()
        .with_env_filter(env_filter)
        .with_file(false)
        .with_line_number(true)
        .with_target(true)
        .init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let args = Cli::parse();
    let task_name = format!("{:?}", args.command);
    let ct = CancellationToken::new();
    let tm = TaskManager::new();
    let t = match args.command {
        Commands::Audio => audio_test(tm.clone(), ct.child_token()),
    };
    info!("Starting task {}.", task_name);
    let r = rt.block_on(async {
        tokio::select! {
            r = tokio::signal::ctrl_c() => { tm.close(); ct.cancel(); r.map_err(|e| Box::new(e).into()) }
            r = t => { r.map_err(|e| e) }
        }
    });
    match r {
        Ok(r) => info!("Task returns {:?}", r),
        Err(e) => error!("Task returns {}", e),
    }
    warn!("Shutting down tokio runtime...");
    rt.shutdown_timeout(Duration::from_secs(5));
    warn!("Stopped.");
    Ok(())
}

// -------

async fn audio_test(
    tm: TaskManager,
    ct: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let _g = ct.drop_guard_ref();
    let mixers = Arc::new(aud::mixer::Mixers::new(tm.clone(), ct.clone()));

    info!("spawn host_handler.");
    let mixers2 = mixers.clone();
    let h = tm.spawn_blocking_typed(move |tm, pu, ct| {
        pu.update(());
        aud::handler::host_handler(tm, mixers2, ct);
        Ok::<(), ()>(())
    });
    h.cancel_at(&ct);

    info!("get device.");
    let dev_id = "pipewire:dc_blocker_sink_EDIFIER_M16_Pro";
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mh = mixers.handle(dev_id).ok_or("No device found.")?;

    info!("test done.");
    tokio::time::sleep(Duration::from_secs(5)).await;
    warn!("Triggering CancellationToken.");
    ct.cancel();
    h.wait_terminal().await;
    Ok(())
}

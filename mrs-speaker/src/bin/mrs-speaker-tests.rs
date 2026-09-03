use clap::{Parser, Subcommand};
use mrs_speaker::aud;
use my_remote_speaker::task::TaskManager;
use std::time::Duration;
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
    AudioHandler,
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
    let t = match args.command {
        Commands::AudioHandler => audio_handler_test(ct.child_token()),
    };
    info!("Starting task {}.", task_name);
    let r = rt.block_on(async {
        tokio::select! {
            r = tokio::signal::ctrl_c() => { ct.cancel(); r.map_err(|e| Box::new(e).into()) }
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

async fn audio_handler_test(ct: CancellationToken) -> Result<(), Box<dyn std::error::Error>> {
    let tm = TaskManager::new();
    let tm2 = tm.clone();
    let ct2 = ct.clone();
    let h = tokio::task::spawn_blocking(move || {
        let mixers = aud::mixer::Mixers::new(tm2.clone(), ct2.clone());
        aud::handler::host_handler(tm2, mixers, ct2);
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    warn!("Triggering CancellationToken.");
    ct.cancel();
    tm.close();
    h.await?;
    Ok(())
}

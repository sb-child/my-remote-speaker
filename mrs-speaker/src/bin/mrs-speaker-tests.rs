use clap::{Args, Parser, Subcommand};
use mrs_speaker::aud;
use my_remote_speaker::task::TaskManager;
use std::time::Duration;
use tracing::info;
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
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let args = Cli::parse();
    let t = match args.command {
        Commands::AudioHandler => audio_handler_test(),
    };
    rt.block_on(t)?;
    info!("Shutting down tokio runtime...");
    rt.shutdown_timeout(Duration::from_secs(5));
    Ok(())
}

// -------

async fn audio_handler_test() -> Result<(), Box<dyn std::error::Error>> {
    let tm = TaskManager::new();
    aud::handler::host_handler(tm.clone(), ());
    tokio::time::sleep(Duration::from_secs(30)).await;
    tm.close();
    Ok(())
}

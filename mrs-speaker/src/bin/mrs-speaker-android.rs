use clap::Parser as _;
use mrs_speaker::{
    android_log::{LogMode, init_tracing},
    android_opts::{Cli, Commands, MagiskCommonArgs},
    magisk_println,
    rmt::{
        self,
        keypair::{KeypairService, key_encode},
        sample_store::SampleStoreService,
    },
};
use std::{error::Error, fs};
use tokio_util::sync::CancellationToken;

fn main() {
    let cli = Cli::parse();
    let log_mode = match &cli.command {
        Commands::Daemon(_) => LogMode::Standard,
        _ => LogMode::Magisk,
    };
    init_tracing(log_mode);
    if let Err(err) = sub_commands(cli.command) {
        tracing::error!("{:#?}", err);
        std::process::exit(1);
    }
}

fn sub_commands(sc: Commands) -> Result<(), Box<dyn Error>> {
    match sc {
        Commands::MagiskInstall(mca) => magisk_installed(mca),
        Commands::MagiskDaemon(mca) => run_magisk_daemon(mca),
        Commands::MagiskAction(mca) => on_magisk_action(mca),
        Commands::MagiskUninstall(mca) => todo!(),
        Commands::Daemon(args) => todo!(),
    }
}

fn magisk_installed(mca: MagiskCommonArgs) -> Result<(), Box<dyn Error>> {
    magisk_println!("Creating config dir...");
    let conf_path = mca.module_path.join("conf");
    fs::create_dir_all(&conf_path)?;
    let kps = rmt::keypair::KeypairService::new(&conf_path)?;
    magisk_println!("Creating keypair...");
    let pubkey_bytes = kps.read_public_key()?;
    magisk_println!("Public key: [{}]", key_encode(&pubkey_bytes));
    magisk_println!("Creating sample database...");
    let _smps = rmt::sample_store::SampleStoreService::new(&conf_path)?;
    magisk_println!("Install done.");
    Ok(())
}

fn run_magisk_daemon(mca: MagiskCommonArgs) -> Result<(), Box<dyn Error>> {
    let conf_path = mca.module_path.join("conf");
    let kps = rmt::keypair::KeypairService::new(&conf_path)?;
    let smps = rmt::sample_store::SampleStoreService::new(&conf_path)?;
    let audio_host = cpal::default_host();
    let rt = tokio::runtime::Builder::new_multi_thread().build()?;
    magisk_println!("Starting daemon.");
    rt.block_on(daemon_app(kps, smps)).unwrap();
    Ok(())
}

async fn daemon_app(kps: KeypairService, smps: SampleStoreService) -> Result<(), Box<dyn Error>> {
    let ct = CancellationToken::new();
    rmt::bind_endpoint(kps, smps, ct).await?;
    Ok(())
}

fn on_magisk_action(mca: MagiskCommonArgs) -> Result<(), Box<dyn Error>> {
    let conf_path = mca.module_path.join("conf");
    let kps = rmt::keypair::KeypairService::new(&conf_path)?;
    let pubkey_bytes = kps.read_public_key()?;
    magisk_println!("Public key: [{}]", key_encode(&pubkey_bytes));
    Ok(())
}

// #[cfg(not(target_os = "android"))]
// fn app() {}

// #[cfg(target_os = "android")]
// fn app() {}

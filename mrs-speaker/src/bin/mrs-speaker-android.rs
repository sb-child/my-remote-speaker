use clap::Parser as _;
use mrs_speaker::{
    android_lib_args::{LaunchMode, LibLaunchArgs},
    android_lib_embed::get_embedded_payload,
    android_log::{LogMode, init_tracing},
    android_opts::{Cli, Commands, DaemonArgs, MagiskCommonArgs},
    magisk_println,
    rmt::{self, keypair::key_encode},
};
use std::{error::Error, fs};
use tracing::{error, info, warn};

fn main() {
    let cli = Cli::parse();
    let log_mode = match &cli.command {
        Commands::Daemon(_) => LogMode::Standard,
        _ => LogMode::Magisk,
    };
    init_tracing(log_mode);
    if let Err(err) = sub_commands(cli.command) {
        error!("Program error: {:#?}({})", err, err);
        std::process::exit(1);
    }
}

fn sub_commands(sc: Commands) -> Result<(), Box<dyn Error>> {
    match sc {
        Commands::MagiskInstall(mca) => magisk_installed(mca),
        Commands::MagiskDaemon(mca) => run_magisk_daemon(mca),
        Commands::MagiskAction(mca) => on_magisk_action(mca),
        Commands::MagiskUninstall(mca) => todo!(),
        Commands::Daemon(args) => run_daemon(args),
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
    info!("Starting as magisk daemon.");
    let conf_path = mca.module_path.join("conf");
    let temp_path = mca.temp_path.join("mrs-temp");
    fs::create_dir_all(&conf_path)?;
    fs::create_dir_all(&temp_path)?;
    let launch_args = LibLaunchArgs {
        launch_mode: LaunchMode::Magisk {
            mod_id: mca.module_id,
            module_path: mca.module_path,
        },
        conf_path,
        temp_path,
    };
    let launch_args_str = serde_json::to_string(&launch_args)?;
    info!("Extract files...");
    // todo
    Ok(())
}

fn run_daemon(da: DaemonArgs) -> Result<(), Box<dyn Error>> {
    info!("Starting as normal daemon.");
    let conf_path = da.conf_path.join("mrs-conf");
    let temp_path = da.temp_path.join("mrs-temp");
    fs::create_dir_all(&conf_path)?;
    fs::create_dir_all(&temp_path)?;
    let launch_args = LibLaunchArgs {
        launch_mode: LaunchMode::Normal,
        conf_path,
        temp_path,
    };
    let launch_args_str = serde_json::to_string(&launch_args)?;
    info!("Extract files...");
    // todo
    Ok(())
}

fn on_magisk_action(mca: MagiskCommonArgs) -> Result<(), Box<dyn Error>> {
    let conf_path = mca.module_path.join("conf");
    let kps = rmt::keypair::KeypairService::new(&conf_path)?;
    let pubkey_bytes = kps.read_public_key()?;
    magisk_println!("Public key: [{}]", key_encode(&pubkey_bytes));
    Ok(())
}

fn extract_embed_files() -> Result<(), Box<dyn Error>> {
    let data = get_embedded_payload()?;
    // todo
    Ok(())
}

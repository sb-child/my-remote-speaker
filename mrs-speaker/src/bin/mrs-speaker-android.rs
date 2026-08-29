use clap::Parser as _;
use mrs_speaker::{
    android_lib_args::{LaunchMode, LibLaunchArgs},
    android_lib_embed::get_embedded_payload,
    android_log::{LogMode, init_tracing},
    android_opts::{Cli, Commands, DaemonArgs, MagiskCommonArgs},
    magisk_println,
    rmt::{self, keypair::key_encode},
};
use std::{
    error::Error,
    fs::{self, File, Permissions},
    io::Write,
    os::unix::{
        fs::PermissionsExt as _,
        process::{CommandExt as _, ExitStatusExt as _},
    },
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tempfile::{NamedTempFile, TempDir};
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
        Commands::MagiskUninstall(mca) => on_magisk_uninstall(mca),
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
            module_path: std::fs::canonicalize(mca.module_path)?,
        },
        conf_path: std::fs::canonicalize(conf_path)?,
        temp_path: std::fs::canonicalize(temp_path)?,
        stop_file: None,
    };
    let base_tmp = select_temp_dir_for_launch(mca.temp_path)?;
    launch_lib(launch_args, &base_tmp)?;
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
        conf_path: std::fs::canonicalize(conf_path)?,
        temp_path: std::fs::canonicalize(temp_path)?,
        stop_file: None,
    };
    let base_tmp = select_temp_dir_for_launch(da.temp_path)?;
    launch_lib(launch_args, &base_tmp)?;
    Ok(())
}

fn select_temp_dir_for_launch(fallback: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
    let base_tmp = PathBuf::from("/data/local/tmp");
    let fb = std::fs::canonicalize(fallback)?.join("mrs-bin-temp");
    let p = match fs::metadata(&base_tmp) {
        Ok(_md) if has_write_permission_in_dir(&base_tmp) => base_tmp,
        Ok(_md) => {
            warn!(
                "{} is readonly, fallback to {}",
                base_tmp.display(),
                fb.display()
            );
            fb
        }
        Err(e) => {
            warn!(
                "{e}: {} is not accessible, fallback to {}",
                base_tmp.display(),
                fb.display()
            );
            fb
        }
    };
    Ok(p)
}

fn has_write_permission_in_dir<P: AsRef<Path>>(dir: P) -> bool {
    let dir = dir.as_ref();
    if !dir.is_dir() {
        return false;
    }
    match NamedTempFile::new_in(dir) {
        Ok(mut temp_file) => {
            if temp_file.write_all(&[1, 2, 3, 4]).is_err() {
                return false;
            }
            if temp_file.flush().is_err() {
                return false;
            }
            true
        }
        Err(_) => false,
    }
}

fn on_magisk_action(mca: MagiskCommonArgs) -> Result<(), Box<dyn Error>> {
    let conf_path = mca.module_path.join("conf");
    let kps = rmt::keypair::KeypairService::new(&conf_path)?;
    let pubkey_bytes = kps.read_public_key()?;
    magisk_println!("Public key: [{}]", key_encode(&pubkey_bytes));
    Ok(())
}

fn on_magisk_uninstall(_mca: MagiskCommonArgs) -> Result<(), Box<dyn Error>> {
    // todo: stop current running process, remove conf and temp files
    Ok(())
}

fn launch_lib(mut launch_args: LibLaunchArgs, base_tmp: &Path) -> Result<i32, Box<dyn Error>> {
    info!("Extracting payload...");
    let data = get_embedded_payload()?;
    info!("Payload extracted.");
    if !base_tmp.exists() {
        warn!(
            "{} not exist. Trying to create this directory.",
            base_tmp.display()
        );
        fs::create_dir_all(base_tmp)?;
    }
    let temp_dir = TempDir::new_in(base_tmp)?;
    info!(
        "Created {} for extracting files...",
        temp_dir.path().display()
    );
    let jar_path = temp_dir.path().join("mrs_speaker_dex.jar");
    let lib_path = temp_dir.path().join("libmrs_speaker.so");
    fs::write(&jar_path, &data.jar_data)?;
    fs::write(&lib_path, &data.lib_data)?;
    info!("Setting permissions...");
    let jar_permissions = Permissions::from_mode(0o400); // read-only
    let lib_permissions = Permissions::from_mode(0o500); // read-only + execute
    fs::set_permissions(&jar_path, jar_permissions)?;
    fs::set_permissions(&lib_path, lib_permissions)?;
    info!("Registered signal handler for Ctrl-C.");
    let running = Arc::new(AtomicBool::new(true));
    let force_stop = Arc::new(AtomicBool::new(false));
    let r = running.clone();
    let f = force_stop.clone();
    ctrlc::set_handler(move || {
        warn!("Ctrl-C detected.");
        if !r.update(Ordering::SeqCst, Ordering::SeqCst, |_x| false) {
            warn!("Double Ctrl-C detected, force stop.");
            f.store(true, Ordering::SeqCst);
        }
    })?;
    info!("Launch app_process.");
    let should_stop_path = temp_dir.path().join("should_stop");
    launch_args.stop_file.replace(should_stop_path.clone()); // don't canonicalize, file not create yet
    let launch_args_str = serde_json::to_string(&launch_args)?;
    let mut cmd = Command::new("/system/bin/app_process");
    cmd.env("CLASSPATH", &jar_path)
        .env("LD_LIBRARY_PATH", temp_dir.path())
        .env("MRS_LIBFILE_PATH", lib_path.to_str().unwrap_or_default());
    cmd.arg(temp_dir.path())
        .arg("com.sbchild.mrs_speaker_android.Main")
        .arg(&launch_args_str);
    cmd.process_group(0);
    let mut child = cmd.spawn()?;
    let mut stop_signaled = false;
    let mut stop_time: Option<Instant> = None;
    info!("Launched app_process. Into Java World.");
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if !running.load(Ordering::SeqCst) && !stop_signaled {
            info!("Creating should_stop file, waiting app_process exit.");
            stop_signaled = true;
            let _ = File::create(&should_stop_path);
            stop_time = Some(Instant::now());
        }
        if force_stop.load(Ordering::SeqCst) {
            error!("Triggering force stop.");
            let _ = child.kill();
            break child.wait()?;
        }
        if stop_signaled {
            if let Some(start_time) = stop_time {
                if start_time.elapsed() >= Duration::from_secs(10) {
                    error!("app_process got stuck, force stop.");
                    let _ = child.kill();
                    break child.wait()?;
                }
            }
        }
        thread::sleep(Duration::from_millis(50));
    };
    let exit_code = match status.code() {
        Some(code) => code,
        None => status.signal().map(|s| 128 + s).unwrap_or(-1),
    };
    warn!("app_process returned exit code {exit_code}.");
    Ok(exit_code)
}

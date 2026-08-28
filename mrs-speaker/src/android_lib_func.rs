use crate::android_lib_args;
use crate::{
    android_log,
    rmt::{
        self,
        keypair::{KeypairService, key_encode},
        sample_cache::SampleCacheService,
        sample_store::SampleStoreService,
    },
};
use cpal::traits::HostTrait as _;
#[cfg(feature = "android")]
use jni::{
    EnvUnowned,
    errors::ThrowRuntimeExAndDefault,
    jni_str,
    objects::{JClass, JObject, JString},
    strings::JNIString,
};
use std::error::Error;
#[cfg(feature = "android")]
use std::ffi::c_void;
use std::{fs, path::PathBuf};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[cfg(feature = "android")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn entrypoint(
    mut maybe_env: EnvUnowned,
    _class: JClass,
    context: JObject,
    json_config: JString,
) {
    eprintln!("=== Into Java Native World ===");
    if context.is_null() {
        eprintln!("Context is null.");
        return;
    }
    let maybe_jvm = maybe_env
        .with_env(|env| {
            let jvm = env.get_java_vm()?;
            Ok::<Option<_>, jni::errors::Error>(Some(jvm))
        })
        .resolve::<ThrowRuntimeExAndDefault>();
    let jvm = match maybe_jvm {
        Some(jvm) => jvm,
        None => {
            eprintln!("Failed to get jvm.");
            return;
        }
    };
    let vm_ptr = jvm.get_raw() as *mut c_void;
    let maybe_global_context = maybe_env
        .with_env(|env| {
            let activity_ptr = env.new_global_ref(&context)?;
            Ok::<Option<_>, jni::errors::Error>(Some(activity_ptr))
        })
        .resolve::<ThrowRuntimeExAndDefault>();
    let global_context = match maybe_global_context {
        Some(global_context) => global_context,
        None => {
            eprintln!("Failed to get global-context.");
            return;
        }
    };
    let activity_ptr = global_context.as_raw() as *mut c_void;
    std::mem::forget(global_context);
    unsafe { ndk_context::initialize_android_context(vm_ptr, activity_ptr) };
    let conf_string = json_config.to_string();
    if !conf_string.starts_with("{") {
        let err_msg = format!("Failed to get config: {conf_string}");
        eprintln!("{err_msg}");
        let _ = maybe_env
            .with_env(|env| {
                let err_msg = JNIString::new(err_msg);
                env.throw_new(jni_str!("java/lang/IllegalArgumentException"), err_msg)?;
                Ok::<_, jni::errors::Error>(())
            })
            .resolve::<ThrowRuntimeExAndDefault>();
        return;
    }
    let conf: android_lib_args::LibLaunchArgs = match serde_json::from_str(&conf_string) {
        Ok(c) => c,
        Err(e) => {
            let err_msg = format!("Failed to parse config: {e}. String is {conf_string}");
            eprintln!("{err_msg}");
            let _ = maybe_env
                .with_env(|env| {
                    let err_msg = JNIString::new(err_msg);
                    env.throw_new(jni_str!("java/lang/IllegalArgumentException"), err_msg)?;
                    Ok::<_, jni::errors::Error>(())
                })
                .resolve::<ThrowRuntimeExAndDefault>();
            return;
        }
    };
    if let Err(err) = lib_main(conf) {
        let err_msg = format!("lib_main error: {:#?}({})", err, err);
        eprintln!("{err_msg}");
        let _ = maybe_env
            .with_env(|env| {
                let err_msg = JNIString::new(err_msg);
                env.throw_new(jni_str!("java/lang/RuntimeException"), err_msg)?;
                Ok::<_, jni::errors::Error>(())
            })
            .resolve::<ThrowRuntimeExAndDefault>();
        return;
    }
}

pub fn lib_main(conf: android_lib_args::LibLaunchArgs) -> Result<(), Box<dyn Error>> {
    let log_mode = match &conf.launch_mode {
        android_lib_args::LaunchMode::Normal => android_log::LogMode::Standard,
        _ => android_log::LogMode::Magisk,
    };
    android_log::init_tracing(log_mode);
    let r = match conf.launch_mode {
        android_lib_args::LaunchMode::Magisk {
            mod_id: _mod_id,
            module_path: _module_path,
        } => run_magisk_daemon(conf.conf_path, conf.temp_path, conf.stop_file),
        android_lib_args::LaunchMode::Normal => {
            run_normal_daemon(conf.conf_path, conf.temp_path, conf.stop_file)
        }
    };
    if let Err(err) = r {
        eprintln!("daemon error: {:#?}({})", err, err);
    }
    Ok(())
}

fn run_magisk_daemon(
    conf_path: PathBuf,
    temp_path: PathBuf,
    stop_file: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    info!("Starting as magisk daemon.");
    fs::create_dir_all(&conf_path)?;
    fs::create_dir_all(&temp_path)?;
    info!("Init components...");
    let kps = rmt::keypair::KeypairService::new(&conf_path)?;
    let smps = SampleStoreService::new(&conf_path)?;
    let smcs = SampleCacheService::new(&temp_path)?;
    // let audio_host = cpal::default_host();
    test_audio();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    info!("Starting daemon...");
    rt.block_on(daemon_app(kps, smps, smcs, stop_file))?;
    Ok(())
}

fn run_normal_daemon(
    conf_path: PathBuf,
    temp_path: PathBuf,
    stop_file: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    info!("Starting as normal daemon.");
    fs::create_dir_all(&conf_path)?;
    fs::create_dir_all(&temp_path)?;
    info!("Init components...");
    let kps = rmt::keypair::KeypairService::new(&conf_path)?;
    let pubkey_bytes = kps.read_public_key()?;
    info!("Public key: [{}]", key_encode(&pubkey_bytes));
    let smps = SampleStoreService::new(&conf_path)?;
    let smcs = SampleCacheService::new(&temp_path)?;
    // let audio_host = cpal::default_host();
    test_audio();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    info!("Starting daemon...");
    rt.block_on(daemon_app(kps, smps, smcs, stop_file))?;
    Ok(())
}

async fn daemon_app(
    kps: KeypairService,
    smps: SampleStoreService,
    smcs: SampleCacheService,
    stop_file: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let ct = CancellationToken::new();
    if let Some(sf) = stop_file {
        tokio::spawn(stop_file_handler(sf, ct.clone()));
    }
    rmt::bind_endpoint(kps, smps, smcs, ct).await?;
    Ok(())
}

async fn stop_file_handler(stop_file: PathBuf, cancel_token: CancellationToken) {
    info!("Listening to {}", stop_file.display());
    let mut check_interval = tokio::time::interval(tokio::time::Duration::from_millis(200));
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                break;
            }
            _ = check_interval.tick() => {
                if tokio::fs::try_exists(&stop_file).await.unwrap_or(false) {
                    info!("File {} Created. Cancelling tasks.", stop_file.display());
                    cancel_token.cancel();
                    break;
                }
            }
        }
    }
}

fn test_audio() {
    let audio_host = cpal::default_host();
    let default_dev = audio_host.default_output_device();
    if let Some(dev) = default_dev {
        info!("default_output_device: {:?}", dev);
    }
    match audio_host.output_devices() {
        Ok(devs) => {
            for d in devs {
                info!("output_device: {:?}", d);
            }
        }
        Err(e) => {
            warn!("Failed to get output_devices: {:?}", e);
        }
    }
}

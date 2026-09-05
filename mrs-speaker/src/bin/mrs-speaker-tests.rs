use clap::{Parser, Subcommand};
use fundsp::prelude::*;
use mrs_speaker::aud::{self, AudioManager, Device, SAMPLE_RATE};
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

/// Audio 子命令选项
#[derive(clap::Args, Debug, Clone)]
pub struct AudioOpts {
    /// 只播放设备 id 包含此子串的设备。过滤虚拟/重复节点。
    #[arg(long)]
    pub device: Option<String>,
    /// 正弦幅度（默认 0.3）
    #[arg(long, default_value_t = 0.3)]
    pub amp: f32,
}

/// sub-commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// 向设备 attach 5 秒正弦再 detach
    Audio(AudioOpts),
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
    let audio_opts = match &args.command {
        Commands::Audio(opts) => opts.clone(),
    };
    let ct = CancellationToken::new();
    let tm = TaskManager::new();
    info!("Starting task {}.", task_name);
    rt.block_on(async {
        let h = tm.spawn_typed(|tm, pu, ct| async move {
            pu.update(());
            app(args.command, audio_opts, tm, ct)
                .await
                .map_err(|e| format!("{}", e))
        });
        h.cancel_at(&ct);
        tokio::select! {
            _r = tokio::signal::ctrl_c() => { error!("ctrl-c trigged."); }
            _r = h.wait_terminal() => {}
        }
        Ok(())
    })
}

async fn app(
    command: Commands,
    opts: AudioOpts,
    tm: TaskManager,
    ct: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = match command {
        Commands::Audio(_) => audio_test(opts, tm.clone(), ct.child_token()),
    };
    t.await
}

// -------

fn sine_track(freq: f32, amp: f32) -> Box<dyn AudioUnit> {
    let mut net = Net::new(0, 2);
    let _sine_id = net.chain(Box::new(
        (sine_hz::<f32>(freq) | sine_hz::<f32>(freq)) * amp,
    ));
    net.set_sample_rate(SAMPLE_RATE as f64);
    Box::new(net.backend())
}

async fn audio_test(
    opts: AudioOpts,
    tm: TaskManager,
    ct: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let _g = ct.drop_guard_ref();
    info!("start audio manager.");
    let am = AudioManager::new(tm, ct.clone()).await?;

    let devices: Vec<Device> = am
        .devices()
        .into_iter()
        .filter(|d| match &opts.device {
            Some(sub) => d.id().contains(sub.as_str()),
            None => {
                !(d.id().starts_with("pipewire:output.")
                    || d.id().starts_with("pipewire:output_default")
                    || d.id().starts_with("pipewire:sink_default"))
            }
        })
        .filter(|d| d.state() != aud::DeviceState::Gone)
        .collect();
    info!(
        "got {} devices: {:?}",
        devices.len(),
        devices
            .iter()
            .map(|d| d.id().to_owned())
            .collect::<Vec<_>>()
    );
    if devices.is_empty() {
        warn!("no usable device, exit.");
        return Ok(());
    }

    info!("attach sine child to all devices (5s).");
    let mut attached: Vec<(String, aud::TrackId)> = Vec::new();
    for d in &devices {
        info!("resume {}", d.id());
        d.resume()?;
        info!("attach.");
        let id = d.attach(sine_track(440.0, opts.amp))?;
        info!("attached. track={:?}", id);
        attached.push((d.id().to_owned(), id));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    info!("detach all.");
    for d in &devices {
        if let Some((_, track_id)) = attached.iter().find(|(id, _)| id == d.id()) {
            match d.detach(*track_id) {
                Ok(_) => {}
                Err(aud::DeviceError::Gone) => {
                    warn!("device gone during playback, skip detach.")
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

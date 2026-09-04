use clap::{Parser, Subcommand};
use mrs_speaker::aud::{
    self, SAMPLE_RATE,
    handler::host_handler,
    mixer::{Clip, ClipGroup, ClipGroupHandle, MixerHandle, Mixers, Track},
};
use my_remote_speaker::task::TaskManager;
use std::{f32::consts::PI, ops::Index, sync::Arc, time::Duration};
use tokio::task::JoinSet;
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
    info!("Starting task {}.", task_name);
    rt.block_on(async {
        let h = tm.spawn_typed(|tm, pu, ct| async move {
            pu.update(());
            app(args.command, tm, ct)
                .await
                .map_err(|e| format!("{}", e))
        });
        h.cancel_at(&ct);
        tokio::select! {
            _r = tokio::signal::ctrl_c() => { error!("ctrl-c trigged."); }
            r = h.wait_terminal() => { error!("Task result: {:?}", r); }
        }
        tm.close();
    });
    warn!("Shutting down tokio runtime...");
    rt.shutdown_timeout(Duration::from_secs(5));
    warn!("Stopped.");
    Ok(())
}

async fn app(
    command: Commands,
    tm: TaskManager,
    ct: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let t = match command {
        Commands::Audio => audio_test(tm.clone(), ct.child_token()),
    };
    t.await
}

// -------

async fn audio_test(
    tm: TaskManager,
    ct: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let _g = ct.drop_guard_ref();
    let mixers = Arc::new(Mixers::new(tm.clone(), ct.clone()));

    info!("spawn host_handler.");
    let mixers2 = mixers.clone();
    let h = tm.spawn_blocking_typed(move |tm, pu, ct| {
        pu.update(());
        host_handler(tm, mixers2, ct);
        Ok::<(), ()>(())
    });
    h.cancel_at(&ct);

    tokio::time::sleep(Duration::from_secs(1)).await;
    let devs: Vec<(String, MixerHandle)> = mixers
        .devices()
        .into_iter()
        .filter(|x| {
            !(x.id.starts_with("pipewire:output.")
                || x.id.starts_with("pipewire:sink_default")
                || x.id.starts_with("pipewire:output_default"))
        })
        .map(|x| {
            info!("device: {}", x.id);
            mixers.handle(&x.id).map(|d| (x.id, d))
        })
        .flatten()
        .collect();
    info!("got {} devices.", devs.len());

    for (dev_id, mh) in devs {
        info!("create track.");
        let (track, th) = Track::new();
        let (clip_left, clh) = Clip::new(generate_440hz_stereo(1., 0.8, 0.0));
        let (clip_right, crh) = Clip::new(generate_440hz_stereo(1., 0.0, 0.8));
        let (clipgroup, cgh) = ClipGroup::new(vec![clip_left, clip_right]);
        th.push_clip_group(clipgroup).await?;

        info!("resume stream.");
        mh.resume().map_err(|e| format!("{:?}", e))?;
        info!("play track.");
        let track = mh.add_tracks(vec![track]).map_err(|e| format!("{:?}", e))?;
        let track_id = track.get(0).ok_or("no track id.")?;
        cgh.done().await;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    let devs: Vec<MixerHandle> = mixers
        .devices()
        .into_iter()
        .filter(|x| {
            !(x.id.starts_with("pipewire:alsa_output")
                || x.id.starts_with("pipewire:output.dc_blocker_sink")
                || x.id.starts_with("pipewire:sink_default")
                || x.id.starts_with("pipewire:output_default"))
        })
        .map(|x| {
            info!("device: {}", x.id);
            mixers.handle(&x.id)
        })
        .flatten()
        .collect();
    info!("got {} devices.", devs.len());

    let mut tracks: Vec<Track> = Vec::new();
    let mut track_handles: Vec<ClipGroupHandle> = Vec::new();
    for _ in 0..devs.len() {
        info!("create tracks.");
        let (track, th) = Track::new();
        let (clip_left, _clh) = Clip::new(generate_440hz_stereo(1., 0.8, 0.0));
        let (clip_right, _crh) = Clip::new(generate_440hz_stereo(1., 0.0, 0.8));
        let (clipgroup, cgh) = ClipGroup::new(vec![clip_left, clip_right]);
        th.push_clip_group(clipgroup).await?;
        tracks.push(track);
        track_handles.push(cgh);
    }

    info!("resume streams.");
    let mut set = JoinSet::new();
    for d in &devs {
        let d = d.clone();
        set.spawn_blocking(move || d.resume().map_err(|e| format!("{:?}", e)));
    }
    while let Some(res) = set.join_next().await {
        res??;
    }

    info!("play tracks.");
    let mut set = JoinSet::new();
    for (d, track) in devs.iter().zip(tracks) {
        let d = d.clone();
        set.spawn_blocking(move || {
            let ids = d.add_tracks(vec![track]).map_err(|e| format!("{:?}", e))?;
            ids.get(0).ok_or("no track id.")?;
            Ok::<(), String>(())
        });
    }
    while let Some(res) = set.join_next().await {
        res??;
    }

    info!("wait complete.");
    let mut set = JoinSet::new();
    for cgh in track_handles {
        set.spawn(async move { cgh.done().await });
    }
    while let Some(res) = set.join_next().await {
        res?;
    }

    info!("test done.");
    tokio::time::sleep(Duration::from_secs(10)).await;
    warn!("Triggering CancellationToken.");
    ct.cancel();
    h.wait_terminal().await;
    Ok(())
}

fn generate_440hz_stereo(duration_secs: f32, left_pan: f32, right_pan: f32) -> Vec<f32> {
    const FREQUENCY: f32 = 440.0;
    let frames = (SAMPLE_RATE as f32 * duration_secs) as usize;
    let total_samples = frames * 2;
    let mut buffer = Vec::with_capacity(total_samples);
    let omega = 2.0 * PI * FREQUENCY / SAMPLE_RATE as f32;
    for i in 0..frames {
        let sample = (i as f32 * omega).sin();
        buffer.push(sample * left_pan);
        buffer.push(sample * right_pan);
    }
    let total_frames = buffer.len() / 2;
    let fade_frames = (SAMPLE_RATE as f32 * 0.005) as usize;
    for i in 0..fade_frames {
        let g = 0.5 * (1.0 - (i as f32 / fade_frames as f32 * PI).cos());
        // fade-in
        buffer[i * 2] *= g;
        buffer[i * 2 + 1] *= g;
        // fade-out
        let j = total_frames - fade_frames + i;
        buffer[j * 2] *= g;
        buffer[j * 2 + 1] *= g;
    }
    buffer
}

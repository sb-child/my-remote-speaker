use std::{
    collections::{HashMap, HashSet},
    thread,
    time::Duration,
};

use cpal::{
    BufferSize, DeviceType, OutputCallbackInfo, SampleFormat, StreamConfig, SupportedOutputConfigs,
    SupportedStreamConfig, SupportedStreamConfigRange,
    traits::{DeviceTrait, HostTrait, StreamTrait as _},
};
use my_remote_speaker::{
    task::{TaskHandle, TaskManager, TypedTaskState},
    util::IteratorExt as _,
};
use snafu::prelude::*;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::aud::{
    dcblocker::DcBlocker,
    mixer::{Mixer, MixerOutput},
};

type DeviceHandles = HashMap<cpal::DeviceId, TaskHandle<(), (), DeviceHandlerError>>;

#[instrument(skip_all, fields(device = dev_id_str))]
fn on_device_add(
    dev_id_str: String,
    dev_id: cpal::DeviceId,
    dh: &mut DeviceHandles,
    tm: &TaskManager,
) {
    let dev_id_2 = dev_id.clone();
    let h = tm.spawn_blocking_typed(move |tc, ct| {
        tc.update(()).ok(); // todo
        device_handler(&dev_id_str, &dev_id_2, (), ct)
    });
    if let Some(old_task) = dh.insert(dev_id, h) {
        warn!("Task started. Cancelling old task.");
        old_task.cancel(); // maybe unreachable
    } else {
        info!("Task started.");
    };
}

#[instrument(skip_all, fields(device = dev_id_str))]
fn on_device_del(
    dev_id_str: String,
    dev_id: cpal::DeviceId,
    dh: &mut DeviceHandles,
    _tm: &TaskManager,
) {
    if let Some(task) = dh.remove(&dev_id) {
        info!("Cancelling task.");
        task.cancel();
    } else {
        warn!("Task not found.");
    };
}

/// - true: set blacklisted
/// - false: no action required
#[instrument(skip_all, fields(device = dev_id_str, blacklisted = blacklisted))]
fn on_device_online(
    dev_id_str: String,
    dev_id: cpal::DeviceId,
    blacklisted: bool,
    dh: &mut DeviceHandles,
    tm: &TaskManager,
) -> bool {
    if let Some(task) = dh.get(&dev_id) {
        let s = get_device_status(dev_id_str.clone(), task.status());
        match s {
            Some(true) => return true,
            Some(false) => {}
            None => return false,
        }
    }
    let dev_id_2 = dev_id.clone();
    let h = tm.spawn_blocking_typed(move |tc, ct| {
        tc.update(()).ok(); // todo
        device_handler(&dev_id_str, &dev_id_2, (), ct)
    });
    if let Some(old_task) = dh.insert(dev_id, h) {
        warn!("Task restarted. Cancelling old task.");
        old_task.cancel(); // maybe unreachable
    } else {
        info!("Task restarted.");
    };
    false
}

/// - Some(true): device unsupported
/// - Some(false): device disconnected
/// - None: no action required
#[instrument(skip_all, fields(device = dev_id_str))]
fn get_device_status(
    dev_id_str: String,
    s: Option<TypedTaskState<(), (), DeviceHandlerError>>,
) -> Option<bool> {
    match s {
        Some(status) => match status {
            TypedTaskState::Pending => None,
            TypedTaskState::Running(_r) => None,
            TypedTaskState::Completed(_c) => {
                warn!("Task completed. Will not restart.");
                None
            }
            TypedTaskState::Failed(f) => match f.as_ref() {
                DeviceHandlerError::DeviceUnavailable => {
                    info!("Failed because of unavailable.");
                    Some(false)
                }
                DeviceHandlerError::DeviceUnsupported => {
                    info!("Failed because of device type unsupported.");
                    Some(true)
                }
                DeviceHandlerError::Stream { source } => match source {
                    StreamHandlerError::DeviceDisconnected { source } => {
                        info!(
                            "Failed because of disconnected during streaming: {}",
                            source
                        );
                        Some(false)
                    }
                    StreamHandlerError::FormatUnsupported { source } => {
                        info!("Failed because of format unsupported: {}", source);
                        Some(true)
                    }
                    StreamHandlerError::OtherDeviceError { source } => {
                        info!("Failed because of other reason: {}", source);
                        Some(true)
                    }
                },
            },
            TypedTaskState::Cancelled => {
                warn!("Cancelled by TaskManager. Will not restart.");
                None
            }
        },
        None => {
            warn!("Deleted by TaskManager. Will not restart.");
            None
        }
    }
}

pub fn host_handler(tm: TaskManager, mixers: (), ct: CancellationToken) {
    let audio_host = cpal::default_host();
    enum Action {
        OnDeviceAdd,
        OnDeviceDel,
        OnDeviceOnline(bool),
    }
    let mut device_handles: DeviceHandles = HashMap::new();
    // `value = true` -> blacklisted (device unsupported)
    let mut prev_devices: HashMap<cpal::DeviceId, bool> = HashMap::new();
    loop {
        let devices_snapshot = match audio_host.output_devices() {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to enumerate output devices: {}", e);
                break;
            }
        };
        let mut actions: HashMap<cpal::DeviceId, Action> = HashMap::new();
        let mut current_devices: HashSet<cpal::DeviceId> = HashSet::new();
        for dev in devices_snapshot {
            let dev_id = match dev.id() {
                Ok(d) => d,
                Err(_) => continue, // device suddenly disconnected
            };
            current_devices.insert(dev_id.clone());
            if let Some(blacklisted) = prev_devices.get(&dev_id) {
                actions.insert(dev_id, Action::OnDeviceOnline(*blacklisted));
            } else {
                actions.insert(dev_id.clone(), Action::OnDeviceAdd);
                prev_devices.insert(dev_id, false);
            }
        }
        prev_devices.retain(|dev_id, _| {
            let is_present = current_devices.contains(dev_id);
            if !is_present {
                actions.insert(dev_id.clone(), Action::OnDeviceDel);
            }
            is_present
        });
        for (dev_id, action) in actions {
            let dev_id_str = dev_id.to_string();
            match action {
                Action::OnDeviceAdd => on_device_add(dev_id_str, dev_id, &mut device_handles, &tm),
                Action::OnDeviceDel => on_device_del(dev_id_str, dev_id, &mut device_handles, &tm),
                Action::OnDeviceOnline(blacklisted) => {
                    let should_blacklist_this = on_device_online(
                        dev_id_str,
                        dev_id.clone(),
                        blacklisted,
                        &mut device_handles,
                        &tm,
                    );
                    if should_blacklist_this {
                        prev_devices.insert(dev_id, true);
                    }
                }
            }
        }
        if ct.is_cancelled() {
            error!("Cancelled. Stopping all handles.");
            for (_, h) in &device_handles {
                h.cancel();
            }
            break;
        } else {
            thread::sleep(Duration::from_secs(1));
        }
    }
}

#[instrument(skip_all, fields(device = _dev_id))]
fn device_handler(
    _dev_id: &str,
    dev_id: &cpal::DeviceId,
    mixer: (),
    ct: CancellationToken,
) -> Result<(), DeviceHandlerError> {
    info!("Init device...");
    let audio_host = cpal::default_host();
    let device = audio_host
        .device_by_id(dev_id)
        .context(DeviceUnavailableSnafu)?;
    ensure!(device.supports_output(), DeviceUnsupportedSnafu);
    debug!("device supports output.");
    // many device reported as Unknown
    // let desc = device.description().ok().context(DeviceUnavailableSnafu)?;
    // ensure!(
    //     matches!(
    //         desc.device_type(),
    //         DeviceType::Dock
    //             | DeviceType::Earpiece
    //             | DeviceType::Handset
    //             | DeviceType::Headphones
    //             | DeviceType::Headset
    //             | DeviceType::HearingAid
    //             | DeviceType::Speaker
    //             | DeviceType::Virtual
    //     ),
    //     DeviceUnsupportedSnafu
    // );
    // debug!("device type supported.");
    let soc = device
        .supported_output_configs()
        .ok()
        .context(DeviceUnsupportedSnafu)?;
    debug!("got supported_output_configs.");
    let sample_rate = 48000;
    let (support_f32, support_2ch) =
        get_supported_config(soc, sample_rate).context(DeviceUnsupportedSnafu)?;
    debug!(
        "config: support_f32={}, support_2ch={}",
        support_f32, support_2ch
    );
    let stream_config = StreamConfig {
        channels: if support_2ch { 2 } else { 1 },
        sample_rate: sample_rate,
        // no guarantees can be made about the actual callback size
        buffer_size: BufferSize::Fixed(256),
    };
    let device_wait_timeout = Duration::from_secs(1);
    let (mixer, mixer_out) = Mixer::new(); // todo
    stream_handler(
        &dev_id.to_string(),
        device,
        stream_config,
        device_wait_timeout,
        support_2ch,
        support_f32,
        mixer_out,
        ct,
    )
    .context(StreamSnafu)?;
    Ok(())
}

#[instrument(skip_all, fields(device = _dev_id, supp_2ch = support_2ch, supp_f32 = support_f32))]
fn stream_handler(
    _dev_id: &str,
    device: cpal::Device,
    stream_config: StreamConfig,
    device_wait_timeout: Duration,
    support_2ch: bool,
    support_f32: bool,
    mixer_out: MixerOutput,
    ct: CancellationToken,
) -> Result<(), StreamHandlerError> {
    loop {
        info!("Building output stream...");
        let mo = mixer_out.clone();
        let err_cb = |err: cpal::Error| {};
        let mut temp_buf: Vec<f32> = vec![];
        let (mut dc_blocker, dc_blocker_handle) = DcBlocker::default_48k();
        let stream_res = if support_f32 {
            device.build_output_stream(
                stream_config,
                move |data: &mut [f32], cbi: &OutputCallbackInfo| {
                    stream_callback_convertor_f32(
                        data,
                        cbi,
                        &mut temp_buf,
                        &mut dc_blocker,
                        support_2ch,
                        &mo,
                    );
                },
                err_cb,
                Some(device_wait_timeout),
            )
        } else {
            device.build_output_stream(
                stream_config,
                move |data: &mut [i16], cbi: &OutputCallbackInfo| {
                    stream_callback_convertor_i16(
                        data,
                        cbi,
                        &mut temp_buf,
                        &mut dc_blocker,
                        support_2ch,
                        &mo,
                    );
                },
                err_cb,
                Some(device_wait_timeout),
            )
        };
        let stream = match stream_res {
            Ok(stream) => stream,
            Err(e) => match e.kind() {
                cpal::ErrorKind::DeviceBusy => {
                    thread::sleep(device_wait_timeout / 2);
                    continue;
                }
                cpal::ErrorKind::DeviceNotAvailable | cpal::ErrorKind::PermissionDenied => {
                    return Err(StreamHandlerError::DeviceDisconnected { source: e });
                }
                cpal::ErrorKind::UnsupportedConfig => {
                    return Err(StreamHandlerError::FormatUnsupported { source: e });
                }
                _ => {
                    return Err(StreamHandlerError::OtherDeviceError { source: e });
                }
            },
        };
        info!("Stream started, waiting for events.");
        loop {
            if ct.is_cancelled() {
                warn!("Cancelled.");
                return Ok(());
            } else {
                thread::sleep(Duration::from_secs(1));
            }
        }

        // todo:
        // wait for mixer commands
        // call dc_blocker_handle.reset() after stream.pause()
        // call dc_blocker_handle.reset() before stream.play()
        // wait for CancellationToken
        // wait any err_cb error happens
    }
    Ok(())
}

fn stream_callback_convertor_f32(
    data: &mut [f32],
    cbi: &OutputCallbackInfo,
    temp_buf: &mut Vec<f32>,
    dc_blocker: &mut DcBlocker,
    support_2ch: bool,
    mixer_out: &MixerOutput,
) -> () {
    if support_2ch {
        stream_callback_handler(data, cbi, dc_blocker, mixer_out);
    } else {
        let target_2ch_len = data.len() * 2;
        temp_buf.resize(target_2ch_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_2ch_len], cbi, dc_blocker, mixer_out);
        for (i, frame) in temp_buf.chunks_exact(2).enumerate() {
            data[i] = (frame[0] + frame[1]) * 0.5;
        }
    }
}

fn stream_callback_convertor_i16(
    data: &mut [i16],
    cbi: &OutputCallbackInfo,
    temp_buf: &mut Vec<f32>,
    dc_blocker: &mut DcBlocker,
    support_2ch: bool,
    mixer_out: &MixerOutput,
) -> () {
    if support_2ch {
        let target_len = data.len();
        temp_buf.resize(target_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_len], cbi, dc_blocker, mixer_out);
        for (out_sample, &in_sample) in data.iter_mut().zip(temp_buf.iter()) {
            *out_sample = f32_to_i16(in_sample);
        }
    } else {
        let target_2ch_len = data.len() * 2;
        temp_buf.resize(target_2ch_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_2ch_len], cbi, dc_blocker, mixer_out);
        for (i, frame) in temp_buf.chunks_exact(2).enumerate() {
            let mono_f32 = (frame[0] + frame[1]) * 0.5;
            data[i] = f32_to_i16(mono_f32);
        }
    }
}

/// f32, 2ch
fn stream_callback_handler(
    data: &mut [f32],
    cbi: &OutputCallbackInfo,
    dc_blocker: &mut DcBlocker,
    mixer_out: &MixerOutput,
) {
    cbi.timestamp();
    mixer_out.read_frames(data);
    // last, pass the dc blocker
    dc_blocker.process_interleaved(data);
}

#[derive(Snafu, Debug)]
pub enum DeviceHandlerError {
    DeviceUnavailable,
    DeviceUnsupported,
    Stream { source: StreamHandlerError },
}

#[derive(Snafu, Debug)]
pub enum StreamHandlerError {
    DeviceDisconnected { source: cpal::Error },
    FormatUnsupported { source: cpal::Error },
    OtherDeviceError { source: cpal::Error },
}

fn get_supported_config(soc: SupportedOutputConfigs, sample_rate: u32) -> Option<(bool, bool)> {
    let (support_f32_2ch, support_i16_2ch, support_f32_1ch, support_i16_1ch) =
        soc.into_iter().find_conditions((
            |x: &SupportedStreamConfigRange| {
                x.sample_format() == SampleFormat::F32
                    && x.contains_rate(sample_rate)
                    && x.channels() == 2
            },
            |x: &SupportedStreamConfigRange| {
                x.sample_format() == SampleFormat::I16
                    && x.contains_rate(sample_rate)
                    && x.channels() == 2
            },
            |x: &SupportedStreamConfigRange| {
                x.sample_format() == SampleFormat::F32
                    && x.contains_rate(sample_rate)
                    && x.channels() == 1
            },
            |x: &SupportedStreamConfigRange| {
                x.sample_format() == SampleFormat::I16
                    && x.contains_rate(sample_rate)
                    && x.channels() == 1
            },
        ));
    if support_f32_2ch {
        Some((true, true))
    } else if support_i16_2ch {
        Some((false, true))
    } else if support_f32_1ch {
        Some((true, false))
    } else if support_i16_1ch {
        Some((false, false))
    } else {
        None
    }
}

#[inline(always)]
fn f32_to_i16(sample: f32) -> i16 {
    let clamped = sample.clamp(-1.0, 1.0);
    if clamped < 0.0 {
        (clamped * 32768.0) as i16
    } else {
        (clamped * 32767.0) as i16
    }
}

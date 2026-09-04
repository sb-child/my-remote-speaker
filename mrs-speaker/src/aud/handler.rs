use crate::aud::{
    SAMPLE_RATE,
    mixer::{DeviceInfo, MixerController, MixerEvent, MixerEventRx, MixerOutput, Mixers},
};
use cpal::{
    BufferSize, OutputCallbackInfo, SampleFormat, StreamConfig, SupportedOutputConfigs,
    SupportedStreamConfigRange,
    traits::{DeviceTrait, HostTrait, StreamTrait as _},
};
use crossfire::{RecvError, RecvTimeoutError, select::Select};
use my_remote_speaker::{
    task::{ProgressUpdater, TaskHandle, TaskManager, TypedTaskState},
    util::IteratorExt as _,
};
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    thread,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

type DeviceHandles = HashMap<cpal::DeviceId, TaskHandle<(), (), DeviceHandlerError>>;

#[instrument(skip_all, fields(device = dev_id_str))]
fn on_device_add(
    dev_id_str: String,
    dev_id: cpal::DeviceId,
    desc: Option<cpal::DeviceDescription>,
    dh: &mut DeviceHandles,
    mixers: &Mixers,
    tm: &TaskManager,
    ct: CancellationToken,
) {
    let dev_id_2 = dev_id.clone();
    let dev_info = DeviceInfo::create(&dev_id_str, desc.as_ref());
    let (_handle, mixer_ctrl, mixer_out) = mixers.get_or_create(&dev_info);
    let h = tm.spawn_blocking_typed(move |_tm, pu, ct| {
        device_handler(&dev_id_str, &dev_id_2, mixer_ctrl, mixer_out, pu, ct)
    });
    h.cancel_at(&ct);
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
    mixers: &Mixers,
    _tm: &TaskManager,
) {
    if let Some(task) = dh.remove(&dev_id) {
        info!("Cancelling task.");
        task.cancel();
    } else {
        warn!("Task not found.");
    };
    mixers.remove(&dev_id_str);
}

/// - true: set blacklisted
/// - false: no action required
#[instrument(skip_all, fields(device = dev_id_str, blacklisted = blacklisted))]
fn on_device_online(
    dev_id_str: String,
    dev_id: cpal::DeviceId,
    blacklisted: bool,
    description: Option<cpal::DeviceDescription>,
    dh: &mut DeviceHandles,
    mixers: &Mixers,
    tm: &TaskManager,
    ct: CancellationToken,
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
    let dev_info = DeviceInfo::create(&dev_id_str, description.as_ref());
    let (_handle, mixer_ctrl, mixer_out) = mixers.get_or_create(&dev_info);
    let h = tm.spawn_blocking_typed(move |_tm, pu, ct| {
        device_handler(&dev_id_str, &dev_id_2, mixer_ctrl, mixer_out, pu, ct)
    });
    h.cancel_at(&ct);
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
    status: TypedTaskState<(), (), DeviceHandlerError>,
) -> Option<bool> {
    match status {
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
        TypedTaskState::Panicked(p) => {
            info!("Task panicked. Will not restart: {}", p);
            None
        }
        TypedTaskState::Cancelling => {
            warn!("Cancelling by TaskManager. Will not restart.");
            None
        }
        TypedTaskState::Cancelled => {
            warn!("Cancelled by TaskManager. Will not restart.");
            None
        }
        TypedTaskState::Invalid => {
            warn!("Deleted by TaskManager. Will not restart.");
            None
        }
    }
}

pub fn host_handler(
    tm: TaskManager,
    pu: ProgressUpdater<()>,
    ct: CancellationToken,
    mixers: Arc<Mixers>,
) {
    let audio_host = cpal::default_host();
    enum Action {
        OnDeviceAdd(Option<cpal::DeviceDescription>),
        OnDeviceDel,
        OnDeviceOnline(bool, Option<cpal::DeviceDescription>),
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
            let description = dev.description().ok();
            current_devices.insert(dev_id.clone());
            if let Some(blacklisted) = prev_devices.get(&dev_id) {
                actions.insert(dev_id, Action::OnDeviceOnline(*blacklisted, description));
            } else {
                actions.insert(dev_id.clone(), Action::OnDeviceAdd(description));
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
                Action::OnDeviceAdd(description) => on_device_add(
                    dev_id_str,
                    dev_id,
                    description,
                    &mut device_handles,
                    &mixers,
                    &tm,
                    ct.child_token(),
                ),
                Action::OnDeviceDel => {
                    on_device_del(dev_id_str, dev_id, &mut device_handles, &mixers, &tm)
                }
                Action::OnDeviceOnline(blacklisted, description) => {
                    let should_blacklist_this = on_device_online(
                        dev_id_str,
                        dev_id.clone(),
                        blacklisted,
                        description,
                        &mut device_handles,
                        &mixers,
                        &tm,
                        ct.child_token(),
                    );
                    if should_blacklist_this {
                        prev_devices.insert(dev_id, true);
                    }
                }
            }
        }
        if ct.is_cancelled() {
            error!("Cancelled.");
            break;
        } else {
            thread::sleep(Duration::from_secs(1));
        }
        pu.update(()); // devices are inited
    }
}

#[instrument(skip_all, fields(device = _dev_id))]
fn device_handler(
    _dev_id: &str,
    dev_id: &cpal::DeviceId,
    mixer_ctrl: MixerController,
    mixer_out: MixerOutput,
    pu: ProgressUpdater<()>,
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
    let (support_f32, support_2ch) =
        get_supported_config(soc, SAMPLE_RATE).context(DeviceUnsupportedSnafu)?;
    debug!(
        "config: support_f32={}, support_2ch={}",
        support_f32, support_2ch
    );
    let stream_config = StreamConfig {
        channels: if support_2ch { 2 } else { 1 },
        sample_rate: SAMPLE_RATE,
        // no guarantees can be made about the actual callback size
        buffer_size: BufferSize::Fixed(256),
    };
    let device_wait_timeout = Duration::from_secs(1);
    stream_handler(
        &dev_id.to_string(),
        device,
        stream_config,
        device_wait_timeout,
        support_2ch,
        support_f32,
        mixer_ctrl,
        mixer_out,
        pu,
        ct,
    )
    .context(StreamSnafu)?;
    Ok(())
}

#[instrument(skip_all, fields(device = dev_id, supp_2ch = support_2ch, supp_f32 = support_f32))]
fn stream_handler(
    dev_id: &str,
    device: cpal::Device,
    stream_config: StreamConfig,
    device_wait_timeout: Duration,
    support_2ch: bool,
    support_f32: bool,
    mixer_ctrl: MixerController,
    mixer_out: MixerOutput,
    pu: ProgressUpdater<()>,
    ct: CancellationToken,
) -> Result<(), StreamHandlerError> {
    let (restart_tx, restart_rx) = crossfire::mpsc::bounded_blocking(16);
    let events_rx = mixer_ctrl.events();
    let mut sel = Select::new();
    sel.add(&restart_rx);
    sel.add(&events_rx);
    loop {
        if ct.is_cancelled() {
            return Ok(());
        }
        info!("Building output stream...");
        mixer_ctrl.reset();
        let dev_id_string = dev_id.to_string();
        let mc_for_err_cb = mixer_ctrl.clone();
        let mut mo_for_stream_cb = mixer_out.clone();
        let restart_tx_cb = restart_tx.clone();
        let mut temp_buf: Vec<f32> = vec![];
        let stream_res = if support_f32 {
            device.build_output_stream(
                stream_config,
                move |data: &mut [f32], cbi: &OutputCallbackInfo| {
                    stream_callback_convertor_f32(
                        data,
                        cbi,
                        &mut temp_buf,
                        support_2ch,
                        &mut mo_for_stream_cb,
                    );
                },
                move |e| stream_error_callback(&dev_id_string, e, &mc_for_err_cb, &restart_tx_cb),
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
                        support_2ch,
                        &mut mo_for_stream_cb,
                    );
                },
                move |e| stream_error_callback(&dev_id_string, e, &mc_for_err_cb, &restart_tx_cb),
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
        // cpal stream 默认是暂停状态。
        if let Err(e) = stream.play() {
            return Err(StreamHandlerError::OtherDeviceError { source: e });
        }
        info!("Stream started, waiting for events.");
        pu.update(()); // device is inited
        loop {
            match stream_handle_event(&mut sel, &restart_rx, &events_rx, &ct) {
                StreamAction::Exit => return Ok(()),
                StreamAction::Restart { source } => {
                    warn!("Device disconnected. Restarting device handler: {}", source);
                    return Err(StreamHandlerError::DeviceDisconnected { source });
                }
                StreamAction::Play => {
                    if let Err(e) = stream.play() {
                        return Err(StreamHandlerError::OtherDeviceError { source: e });
                    }
                }
                StreamAction::Pause => {
                    if let Err(e) = stream.pause() {
                        return Err(StreamHandlerError::OtherDeviceError { source: e });
                    }
                }
                StreamAction::Ignore => {}
            }
        }
    }
}

/// stream_handler 事件循环的处理结果
enum StreamAction {
    /// ct 取消，正常退出。
    Exit,
    /// 重启 device handler。
    Restart { source: cpal::Error },
    /// mixer 请求恢复流。
    Play,
    /// mixer 请求暂停流。
    Pause,
    /// 无动作。
    Ignore,
}

/// 等待并处理一个事件，返回对应动作。
fn stream_handle_event(
    sel: &mut Select<'_>,
    restart_rx: &crossfire::Rx<crossfire::mpsc::Array<cpal::Error>>,
    events_rx: &MixerEventRx,
    ct: &CancellationToken,
) -> StreamAction {
    match sel.select_timeout(Duration::from_millis(100)) {
        Ok(res) => {
            if res == *restart_rx {
                match restart_rx.read_select(res) {
                    Ok(e) => StreamAction::Restart { source: e },
                    Err(RecvError) => StreamAction::Ignore, // 不应该
                }
            } else if res == *events_rx {
                match events_rx.read_select(res) {
                    Ok(MixerEvent::RequestStandby) => StreamAction::Pause,
                    Ok(MixerEvent::RequestResume) => StreamAction::Play,
                    // mixer 死了，events_rx 断了。
                    Err(RecvError) => StreamAction::Ignore,
                }
            } else {
                StreamAction::Ignore
            }
        }
        Err(RecvTimeoutError::Timeout) => {
            if ct.is_cancelled() {
                StreamAction::Exit
            } else {
                StreamAction::Ignore
            }
        }
        Err(RecvTimeoutError::Disconnected) => StreamAction::Exit,
    }
}

#[instrument(skip_all, fields(device = _dev_id))]
fn stream_error_callback(
    _dev_id: &str,
    err: cpal::Error,
    mc: &MixerController,
    restart_tx: &crossfire::MTx<crossfire::mpsc::Array<cpal::Error>>,
) {
    match err.kind() {
        e @ (cpal::ErrorKind::DeviceBusy
        | cpal::ErrorKind::DeviceNotAvailable
        | cpal::ErrorKind::HostUnavailable
        | cpal::ErrorKind::PermissionDenied
        | cpal::ErrorKind::ResourceExhausted
        | cpal::ErrorKind::StreamInvalidated
        | cpal::ErrorKind::BackendError
        | cpal::ErrorKind::Other) => {
            // restart
            mc.disconnected();
            error!("got error, restarting device handler: {}", e);
            restart_tx.send(err).ok();
        }
        e => {
            warn!("ignored error: {}", e);
            // ignore
        }
    };
}

fn stream_callback_convertor_f32(
    data: &mut [f32],
    cbi: &OutputCallbackInfo,
    temp_buf: &mut Vec<f32>,
    support_2ch: bool,
    mixer_out: &mut MixerOutput,
) -> () {
    if support_2ch {
        // build_output_stream docs: The slice is pre-filled with silence.
        stream_callback_handler(data, cbi, mixer_out);
    } else {
        let target_2ch_len = data.len() * 2;
        temp_buf.resize(target_2ch_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_2ch_len], cbi, mixer_out);
        for (i, frame) in temp_buf.chunks_exact(2).enumerate() {
            data[i] = (frame[0] + frame[1]) * 0.5;
        }
    }
}

fn stream_callback_convertor_i16(
    data: &mut [i16],
    cbi: &OutputCallbackInfo,
    temp_buf: &mut Vec<f32>,
    support_2ch: bool,
    mixer_out: &mut MixerOutput,
) -> () {
    if support_2ch {
        let target_len = data.len();
        temp_buf.resize(target_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_len], cbi, mixer_out);
        for (out_sample, &in_sample) in data.iter_mut().zip(temp_buf.iter()) {
            *out_sample = f32_to_i16(in_sample);
        }
    } else {
        let target_2ch_len = data.len() * 2;
        temp_buf.resize(target_2ch_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_2ch_len], cbi, mixer_out);
        for (i, frame) in temp_buf.chunks_exact(2).enumerate() {
            let mono_f32 = (frame[0] + frame[1]) * 0.5;
            data[i] = f32_to_i16(mono_f32);
        }
    }
}

/// f32, 2ch
fn stream_callback_handler(
    data: &mut [f32],
    _cbi: &OutputCallbackInfo,
    mixer_out: &mut MixerOutput,
) {
    // let t = cbi.timestamp();
    // let read_timeout = t.playback.duration_since(t.callback);
    mixer_out.read_frames(data);
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

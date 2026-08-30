use std::{thread, time::Duration};

use cpal::{
    BufferSize, DeviceType, OutputCallbackInfo, SampleFormat, StreamConfig, SupportedOutputConfigs,
    SupportedStreamConfig, SupportedStreamConfigRange,
    traits::{DeviceTrait, HostTrait, StreamTrait as _},
};
use my_remote_speaker::{task::TaskManager, util::IteratorExt as _};
use snafu::prelude::*;

use crate::aud::dcblocker::DcBlocker;

fn host_handler(tm: TaskManager, mixers: ()) {
    let audio_host = cpal::default_host();
    // looking for devices every 1s
    // start device_handler with TaskManager
    // DeviceDisconnected / DeviceUnavailable -> wait a while then retry
    // DeviceUnsupported / FormatUnsupported / OtherDeviceError -> ignore until device disconnect and appear again
}

fn device_handler(dev_id: &cpal::DeviceId, mixer: ()) -> Result<(), DeviceHandlerError> {
    let audio_host = cpal::default_host();
    let device = audio_host
        .device_by_id(dev_id)
        .context(DeviceUnavailableSnafu)?;
    let desc = device.description().ok().context(DeviceUnavailableSnafu)?;
    ensure!(device.supports_output(), DeviceUnsupportedSnafu);
    ensure!(
        matches!(
            desc.device_type(),
            DeviceType::Dock
                | DeviceType::Earpiece
                | DeviceType::Handset
                | DeviceType::Headphones
                | DeviceType::Headset
                | DeviceType::HearingAid
                | DeviceType::Speaker
                | DeviceType::Virtual
        ),
        DeviceUnsupportedSnafu
    );
    let soc = device
        .supported_output_configs()
        .ok()
        .context(DeviceUnsupportedSnafu)?;
    let sample_rate = 48000;
    let (support_f32, support_2ch) =
        get_supported_config(soc, sample_rate).context(DeviceUnsupportedSnafu)?;
    let stream_config = StreamConfig {
        channels: if support_2ch { 2 } else { 1 },
        sample_rate: sample_rate,
        // no guarantees can be made about the actual callback size
        buffer_size: BufferSize::Fixed(256),
    };
    let device_wait_timeout = Duration::from_secs(1);
    stream_handler(
        device,
        stream_config,
        device_wait_timeout,
        support_2ch,
        support_f32,
        mixer,
    )
    .context(StreamSnafu)?;
    Ok(())
}

fn stream_handler(
    device: cpal::Device,
    stream_config: StreamConfig,
    device_wait_timeout: Duration,
    support_2ch: bool,
    support_f32: bool,
    mixer: (),
) -> Result<(), StreamHandlerError> {
    loop {
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
                        mixer,
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
                        mixer,
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
    mixer: (),
) -> () {
    if support_2ch {
        stream_callback_handler(data, cbi, dc_blocker, mixer);
    } else {
        let target_2ch_len = data.len() * 2;
        temp_buf.resize(target_2ch_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_2ch_len], cbi, dc_blocker, mixer);
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
    mixer: (),
) -> () {
    if support_2ch {
        let target_len = data.len();
        temp_buf.resize(target_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_len], cbi, dc_blocker, mixer);
        for (out_sample, &in_sample) in data.iter_mut().zip(temp_buf.iter()) {
            *out_sample = f32_to_i16(in_sample);
        }
    } else {
        let target_2ch_len = data.len() * 2;
        temp_buf.resize(target_2ch_len, 0.0);
        stream_callback_handler(&mut temp_buf[..target_2ch_len], cbi, dc_blocker, mixer);
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
    mixer: (),
) {
    cbi.timestamp();
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

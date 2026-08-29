use std::{thread, time::Duration};

use cpal::{
    BufferSize, DeviceType, OutputCallbackInfo, StreamConfig,
    traits::{DeviceTrait, HostTrait, StreamTrait as _},
};
use snafu::prelude::*;

fn host_handler() {
    let audio_host = cpal::default_host();
}

fn device_handler(dev_id: &cpal::DeviceId) -> Result<(), DeviceHandlerError> {
    let audio_host = cpal::default_host();
    let device = audio_host
        .device_by_id(dev_id)
        .context(DeviceUnavailableSnafu)?;
    let desc = device.description().ok().context(DeviceUnavailableSnafu)?;
    ensure!(device.supports_output(), DeviceUnsupportedSnafu);
    ensure!(
        !matches!(
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
    let _soc = device
        .supported_output_configs()
        .ok()
        .context(DeviceUnsupportedSnafu)?;
    // soc.into_iter()
    //     .filter(|x| x.try_with_sample_rate(48000).is_some());
    // for c in soc {
    //     c.channels();
    //     c.buffer_size();
    //     c.try_with_sample_rate(48000);
    //     c.sample_format();
    // }
    let stream_config = StreamConfig {
        channels: 2,
        sample_rate: 48000,
        buffer_size: BufferSize::Fixed(128),
    };
    let device_wait_timeout = Duration::from_secs(1);
    stream_handler(device, stream_config, device_wait_timeout).context(StreamSnafu)?;
    Ok(())
}

fn stream_handler(
    device: cpal::Device,
    stream_config: StreamConfig,
    device_wait_timeout: Duration,
) -> Result<(), StreamHandlerError> {
    loop {
        let data_cb = |data: &mut [f32], cbi: &OutputCallbackInfo| {
            cbi.timestamp();
        };
        let err_cb = |err: cpal::Error| {};
        let stream_res =
            device.build_output_stream(stream_config, data_cb, err_cb, Some(device_wait_timeout));
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
        stream.now();
        // stream.pause();
        // todo: wait for any error happens
    }
    Ok(())
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

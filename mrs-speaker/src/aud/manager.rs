use cpal::{
    BufferSize, DeviceType, StreamConfig,
    traits::{DeviceTrait, HostTrait},
};
use snafu::prelude::*;

pub struct AudioManager {}

impl AudioManager {}

fn host_handler() {
    let audio_host = cpal::default_host();
}

fn device_handler(dev_id: &cpal::DeviceId) -> Result<(), DeviceHandlerError> {
    let audio_host = cpal::default_host();
    let device = audio_host
        .device_by_id(dev_id)
        .context(DeviceNotAvailableSnafu)?;
    ensure!(device.supports_output(), UnsupportedSnafu);
    let desc = device.description().ok().context(DeviceNotAvailableSnafu)?;
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
        UnsupportedSnafu
    );
    let soc = device
        .supported_output_configs()
        .ok()
        .context(UnsupportedSnafu)?;
    soc.into_iter()
        .filter(|x| x.try_with_sample_rate(48000).is_some());
    // for c in soc {
    //     c.channels();
    //     c.buffer_size();
    //     c.try_with_sample_rate(48000);
    //     c.sample_format();
    // }

    // let stream_config = StreamConfig {
    //     channels: 2,
    //     sample_rate: 48000,
    //     buffer_size: BufferSize::default(),
    // };
    // device.build_output_stream(, data_callback, error_callback, timeout);
    Ok(())
}

#[derive(Snafu, Debug)]
pub enum DeviceHandlerError {
    DeviceNotAvailable,
    Unsupported,
}

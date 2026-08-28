#[cfg(feature = "android")]
use jni::{
    EnvUnowned,
    errors::ThrowRuntimeExAndDefault,
    objects::{JClass, JObject, JString},
};
#[cfg(feature = "android")]
use std::error::Error;
#[cfg(feature = "android")]
use std::ffi::c_void;

pub mod android_lib_args;
pub mod android_lib_func;
pub mod android_log;
pub mod android_opts;
pub mod conf;
pub mod rmt;

#[cfg(feature = "android")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Java_com_sbchild_mrs_1speaker_1android_Main_launchMrsSpeakerAndroid(
    maybe_env: EnvUnowned,
    _class: JClass,
    context: JObject,
    json_config: JString,
) {
    unsafe { android_lib_func::entrypoint(maybe_env, _class, context, json_config) }
}

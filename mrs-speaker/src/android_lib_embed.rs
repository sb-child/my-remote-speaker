use serde::Deserialize;
use serde_with::{base64::Base64, serde_as};
use sha2::{Digest as _, Sha256};
use snafu::prelude::*;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

#[serde_as]
#[derive(Deserialize, Debug)]
pub struct Payload {
    #[serde_as(as = "Base64")]
    pub jar_digest: Vec<u8>,
    #[serde_as(as = "Base64")]
    pub jar_data: Vec<u8>,
    #[serde_as(as = "Base64")]
    pub lib_digest: Vec<u8>,
    #[serde_as(as = "Base64")]
    pub lib_data: Vec<u8>,
}

impl Payload {
    pub fn validate(&self) -> Result<(), EmbedError> {
        ensure!(
            self.jar_digest.len() == 32,
            InvalidDigestLengthSnafu {
                field: "jar_digest",
                actual: self.jar_digest.len(),
            }
        );
        ensure!(
            self.lib_digest.len() == 32,
            InvalidDigestLengthSnafu {
                field: "lib_digest",
                actual: self.lib_digest.len(),
            }
        );
        ensure!(
            !self.jar_data.is_empty(),
            EmptyDataSnafu {
                field: "jar_digest",
            }
        );
        ensure!(
            !self.lib_data.is_empty(),
            EmptyDataSnafu {
                field: "lib_digest",
            }
        );
        let calculated_jar_digest = Sha256::digest(&self.jar_data);
        let calculated_lib_digest = Sha256::digest(&self.lib_data);
        ensure!(
            calculated_jar_digest.as_slice() == self.jar_digest.as_slice(),
            DigestMismatchSnafu {
                field: "jar_digest",
            }
        );
        ensure!(
            calculated_lib_digest.as_slice() == self.lib_digest.as_slice(),
            DigestMismatchSnafu {
                field: "lib_digest",
            }
        );
        Ok(())
    }
}

const MAGIC: &[u8; 8] = b"MRS-Data";
const TRAILER_LEN: u64 = 16;

#[derive(Snafu, Debug)]
pub enum EmbedError {
    #[snafu(display("Failed to get current executable path: {}", source))]
    GetCurrentExe { source: std::io::Error },

    #[snafu(display("Failed to open self at {}: {}", fp.display(), source))]
    OpenSelf { source: std::io::Error, fp: PathBuf },

    #[snafu(display("Failed to read self metadata at {}: {}", fp.display(), source))]
    ReadSelfMetadata { source: std::io::Error, fp: PathBuf },

    #[snafu(display("Failed to seek at {}: {}", fp.display(), source))]
    Seek { source: std::io::Error, fp: PathBuf },

    #[snafu(display("Failed to read at {}: {}", fp.display(), source))]
    Read { source: std::io::Error, fp: PathBuf },

    #[snafu(display("File is too small at {}", fp.display()))]
    FileSmall { fp: PathBuf },

    #[snafu(display("Magic bytes not found at {}", fp.display()))]
    NoMagic { fp: PathBuf },

    #[snafu(display("Failed to convert payload size at {}: {}", fp.display(), source))]
    SizeConvert {
        source: std::array::TryFromSliceError,
        fp: PathBuf,
    },

    #[snafu(display("Payload size is incorrect at {}", fp.display()))]
    IncorrectSize { fp: PathBuf },

    #[snafu(display("Failed to parse json at {}: {}", fp.display(), source))]
    JsonParse {
        source: serde_json::Error,
        fp: PathBuf,
    },

    #[snafu(display(
        "Invaild digest length: field={}, length={}, expect 32.",
        field,
        actual
    ))]
    InvalidDigestLength { field: String, actual: usize },

    #[snafu(display("Data is empty: field={}.", field))]
    EmptyData { field: String },

    #[snafu(display("Digest mismatch: field={}.", field))]
    DigestMismatch { field: String },
}

pub fn get_embedded_payload() -> Result<Payload, EmbedError> {
    let path = if cfg!(target_os = "linux") {
        "/proc/self/exe".to_owned()
    } else {
        std::env::current_exe()
            .context(GetCurrentExeSnafu)?
            .to_string_lossy()
            .into_owned()
    };
    let fp = PathBuf::from(&path);
    let mut f = File::open(&path).context(OpenSelfSnafu { fp: &fp })?;
    let file_len = f
        .metadata()
        .context(ReadSelfMetadataSnafu { fp: &fp })?
        .len();
    ensure!(file_len >= TRAILER_LEN, FileSmallSnafu { fp: &fp });
    let mut tail = [0u8; 16];
    f.seek(SeekFrom::End(-(TRAILER_LEN as i64)))
        .context(SeekSnafu { fp: &fp })?;
    f.read_exact(&mut tail).context(ReadSnafu { fp: &fp })?;
    ensure!(&tail[8..16] == MAGIC, NoMagicSnafu { fp: &fp });
    let n =
        u64::from_le_bytes(tail[..8].try_into().context(SizeConvertSnafu { fp: &fp })?) as usize;
    ensure!(
        n as u64 + TRAILER_LEN <= file_len,
        IncorrectSizeSnafu { fp: &fp }
    );
    let mut buf = vec![0u8; n];
    f.seek(SeekFrom::End(-(TRAILER_LEN as i64) - n as i64))
        .context(SeekSnafu { fp: &fp })?;
    f.read_exact(&mut buf).context(ReadSnafu { fp: &fp })?;
    let payload: Payload = serde_json::from_slice(&buf).context(JsonParseSnafu { fp: &fp })?;
    Ok(payload)
}

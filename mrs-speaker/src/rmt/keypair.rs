use crate::conf;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use iroh::{KeyParsingError, PublicKey, SecretKey};
use snafu::prelude::*;
use std::{
    fs::{self, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
};

#[derive(Snafu, Debug)]
pub enum KeypairError {
    #[snafu(display("Directory not found: {}", fp.display()))]
    DirectoryNotFound { fp: PathBuf },

    #[snafu(display("Failed to open keypair file at {}: {}", fp.display(), source))]
    OpenFile { source: std::io::Error, fp: PathBuf },

    #[snafu(display("Failed to read keypair file at {}: {}", fp.display(), source))]
    ReadFile { source: std::io::Error, fp: PathBuf },

    #[snafu(display("Failed to write keypair file at {}: {}", fp.display(), source))]
    WriteFile { source: std::io::Error, fp: PathBuf },

    #[snafu(display("Failed to parse JSON at {}: {}", fp.display(), source))]
    ParseJson {
        source: serde_json::Error,
        fp: PathBuf,
    },

    #[snafu(display("Failed to serialize JSON: {}", source))]
    SerializeJson { source: serde_json::Error },

    #[snafu(display("Failed to parse public key {}: {}", key_encode(pub_bytes), source))]
    ParsePublicKey {
        pub_bytes: Vec<u8>,
        source: KeyParsingError,
    },

    #[snafu(display(
        "Public key {} does not match secret key {}",
        key_encode(pub_bytes),
        key_encode(pri_bytes)
    ))]
    KeypairMismatch {
        pub_bytes: Vec<u8>,
        pri_bytes: Vec<u8>,
    },
}

pub fn key_encode<T: AsRef<[u8]>>(data: &T) -> String {
    BASE64_STANDARD.encode(data)
}

pub struct KeypairService {
    conf_file_path: PathBuf,
}

impl KeypairService {
    pub fn new<P: AsRef<Path>>(conf_base: P) -> Result<Self, KeypairError> {
        let base_path = conf_base.as_ref();
        if !base_path.is_dir() {
            return Err(KeypairError::DirectoryNotFound {
                fp: base_path.to_path_buf(),
            });
        }
        let conf_file_path = base_path.join("keypair.json");
        let service = Self { conf_file_path };
        if service.load_conf().is_err() {
            service.rotate()?;
        }
        Ok(service)
    }

    pub fn read_secret_key(&self) -> Result<SecretKey, KeypairError> {
        let conf = self.load_or_recover()?;
        Ok(SecretKey::from_bytes(&conf.private_part))
    }

    pub fn read_public_key(&self) -> Result<PublicKey, KeypairError> {
        let conf = self.load_or_recover()?;
        Ok(
            PublicKey::from_bytes(&conf.public_part).context(ParsePublicKeySnafu {
                pub_bytes: conf.public_part.to_vec(),
            })?,
        )
    }

    pub fn rotate(&self) -> Result<conf::KeypairConf, KeypairError> {
        let new_conf = Self::generate_new_keypair();
        self.save_conf(&new_conf)?;
        Ok(new_conf)
    }

    /// 尝试读取配置，读取失败则重新生成
    fn load_or_recover(&self) -> Result<conf::KeypairConf, KeypairError> {
        match self.load_conf() {
            Ok(conf) => Ok(conf),
            Err(_) => self.rotate(),
        }
    }

    fn load_conf(&self) -> Result<conf::KeypairConf, KeypairError> {
        let content = fs::read_to_string(&self.conf_file_path).context(ReadFileSnafu {
            fp: &self.conf_file_path,
        })?;
        let conf: conf::KeypairConf = serde_json::from_str(&content).context(ParseJsonSnafu {
            fp: &self.conf_file_path,
        })?;
        let pub_part = PublicKey::from_bytes(&conf.public_part).context(ParsePublicKeySnafu {
            pub_bytes: conf.public_part.to_vec(),
        })?;
        let pri_part = SecretKey::from_bytes(&conf.private_part);
        ensure!(
            pri_part.public() == pub_part,
            KeypairMismatchSnafu {
                pub_bytes: pub_part.to_vec(),
                pri_bytes: pri_part.to_bytes(),
            }
        );
        Ok(conf)
    }

    /// 将配置写入文件
    fn save_conf(&self, conf: &conf::KeypairConf) -> Result<(), KeypairError> {
        let json_str = serde_json::to_string_pretty(conf).context(SerializeJsonSnafu)?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.conf_file_path)
            .context(OpenFileSnafu {
                fp: &self.conf_file_path,
            })?;
        file.write_all(json_str.as_bytes())
            .context(WriteFileSnafu {
                fp: &self.conf_file_path,
            })?;
        Ok(())
    }

    fn generate_new_keypair() -> conf::KeypairConf {
        let prikey = iroh::SecretKey::generate();
        let pubkey = prikey.public();
        conf::KeypairConf {
            public_part: *pubkey.as_bytes(),
            private_part: prikey.to_bytes(),
        }
    }
}

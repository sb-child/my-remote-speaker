use snafu::prelude::*;
use std::{
    fs,
    path::{Path, PathBuf},
};

pub struct SampleStoreService {
    store_path: PathBuf,
}

#[derive(Snafu, Debug)]
pub enum SampleStoreError {
    #[snafu(display("Directory not found: {}", fp.display()))]
    DirectoryNotFound { fp: PathBuf },

    #[snafu(display("Failed to load database {}: {}", fp.display(),source))]
    LoadDatabase {
        source: surrealkv::Error,
        fp: PathBuf,
    },

    #[snafu(display("Failed to remove dir at {}: {}", fp.display(), source))]
    RemoveDir { source: std::io::Error, fp: PathBuf },

    #[snafu(display("Failed to create dir at {}: {}", fp.display(), source))]
    CreateDir { source: std::io::Error, fp: PathBuf },

    #[snafu(display("Failed to start runtime: {}", source))]
    StartRuntime { source: std::io::Error },
}

impl SampleStoreService {
    pub fn new<P: AsRef<Path>>(conf_base: P) -> Result<Self, SampleStoreError> {
        let base_path = conf_base.as_ref();
        if !base_path.is_dir() {
            return Err(SampleStoreError::DirectoryNotFound {
                fp: base_path.to_path_buf(),
            });
        }
        let store_path = base_path.join("sample-store");
        let r = Self {
            store_path: store_path,
        };
        if r.test_db().is_err() {
            r.rebuild_db()?;
        }
        Ok(r)
    }

    fn db_builder(&self) -> surrealkv::TreeBuilder {
        surrealkv::TreeBuilder::new()
            .with_path(self.store_path.clone())
            .with_enable_vlog(true)
            .with_vlog_checksum_verification(surrealkv::VLogChecksumLevel::Full)
    }

    pub async fn open(&self) -> Result<surrealkv::Tree, SampleStoreError> {
        let db = self.db_builder().build().context(LoadDatabaseSnafu {
            fp: self.store_path.clone(),
        })?;
        Ok(db)
    }

    fn test_db(&self) -> Result<(), SampleStoreError> {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_e) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .name("samplestore-test-rt")
                    .build()
                    .context(StartRuntimeSnafu)?;
                rt.handle().clone()
            }
        };
        let r: Result<(), SampleStoreError> = handle.block_on(async {
            let db = self.db_builder().build().context(LoadDatabaseSnafu {
                fp: self.store_path.clone(),
            })?;
            db.close().await.context(LoadDatabaseSnafu {
                fp: self.store_path.clone(),
            })?;
            Ok(())
        });
        r
    }

    fn rebuild_db(&self) -> Result<(), SampleStoreError> {
        fs::remove_dir_all(self.store_path.clone()).context(RemoveDirSnafu {
            fp: self.store_path.clone(),
        })?;
        fs::create_dir_all(self.store_path.clone()).context(CreateDirSnafu {
            fp: self.store_path.clone(),
        })?;
        self.test_db()
    }
}

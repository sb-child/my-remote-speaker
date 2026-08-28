use snafu::prelude::*;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tracing::info;

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
        info!("Service ready.");
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
        info!("Database sample-store opened.");
        Ok(db)
    }

    fn test_db(&self) -> Result<(), SampleStoreError> {
        let action = async {
            let db = self.db_builder().build().context(LoadDatabaseSnafu {
                fp: self.store_path.clone(),
            })?;
            db.close().await.context(LoadDatabaseSnafu {
                fp: self.store_path.clone(),
            })?;
            Ok(())
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(action))
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .name("samplestore-test-rt")
                .enable_all()
                .build()
                .context(StartRuntimeSnafu)?;
            let result = rt.block_on(action);
            rt.shutdown_timeout(std::time::Duration::from_secs(5));
            result
        }
    }

    fn rebuild_db(&self) -> Result<(), SampleStoreError> {
        if self.store_path.exists() {
            fs::remove_dir_all(&self.store_path).context(RemoveDirSnafu {
                fp: self.store_path.clone(),
            })?;
        }
        fs::create_dir_all(self.store_path.clone()).context(CreateDirSnafu {
            fp: self.store_path.clone(),
        })?;
        self.test_db()?;
        info!("New sample-store created.");
        Ok(())
    }
}

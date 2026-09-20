//! Shared admission limit for local and remote footer reads.

use std::{collections::BTreeMap, num::NonZeroUsize, path::PathBuf, sync::Arc};

use tokio::sync::Semaphore;

use crate::parquet_helpers::{default_metadata_parser, Error, FileMass, MetadataParser};

/// A cloneable footer reader whose clones share one global concurrency limit.
#[derive(Clone)]
pub struct FooterReader {
    permits: Arc<Semaphore>,
    concurrency: NonZeroUsize,
}

impl FooterReader {
    /// Limit simultaneous file reads across all users of this reader.
    pub fn new(concurrency: NonZeroUsize) -> Result<Self, Error> {
        if concurrency.get() > Semaphore::MAX_PERMITS {
            return Err(Error("file concurrency exceeds the supported limit".into()));
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(concurrency.get())),
            concurrency,
        })
    }

    /// Maximum number of simultaneous footer reads.
    pub fn concurrency(&self) -> usize {
        self.concurrency.get()
    }

    /// Read one exact path or URI. Local I/O runs on Tokio's blocking pool.
    ///
    /// # Errors
    /// Returns an error for unsupported storage, I/O failures or invalid Parquet.
    pub async fn read(
        &self,
        input: &str,
        options: &BTreeMap<String, String>,
    ) -> Result<(u64, FileMass), Error> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Error(e.to_string()))?;
        let path = if input.contains("://") {
            let url = url::Url::parse(input).map_err(|e| Error(e.to_string()))?;
            if url.scheme() != "file" {
                return super::remote::read_remote_with_options(input, options.clone()).await;
            }
            url.to_file_path()
                .map_err(|()| Error("invalid file URI".into()))?
        } else {
            PathBuf::from(input)
        };
        tokio::task::spawn_blocking(move || {
            // Keep the permit until physical I/O finishes, even if the caller cancels.
            let _permit = permit;
            let size = std::fs::metadata(&path)
                .map_err(|e| Error(e.to_string()))?
                .len();
            let mass = default_metadata_parser().read_masses(&path)?;
            Ok((size, mass))
        })
        .await
        .map_err(|e| Error(format!("footer task failed: {e}")))?
    }
}

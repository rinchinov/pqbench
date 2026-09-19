//! Footer-only Parquet metadata reads over an [`object_store::ObjectStore`].

use object_store::{path::Path, ObjectStore, ObjectStoreExt};

use crate::parquet_helpers::{read_footer_masses, Error, FileMass};

const PARQUET_FOOTER_SIZE: u64 = 8;

/// Read a Parquet object's size and byte masses without fetching data pages.
///
/// This issues one object metadata request followed by two bounded reads: the
/// eight-byte Parquet trailer and the serialized metadata immediately before
/// it. The returned size is from object metadata, which lets callers compare
/// it with an expected value from a table log.
pub async fn read_object_store_masses(
    store: &dyn ObjectStore,
    location: &Path,
) -> Result<(u64, FileMass), Error> {
    let size = store.head(location).await.map_err(object_store_error)?.size;
    if size < PARQUET_FOOTER_SIZE {
        return Err(Error(format!(
            "object {location} is too small to be a Parquet file: {size} bytes"
        )));
    }

    let trailer = store
        .get_range(location, size - PARQUET_FOOTER_SIZE..size)
        .await
        .map_err(object_store_error)?;
    if trailer.len() != PARQUET_FOOTER_SIZE as usize {
        return Err(Error(format!(
            "object {location} returned a truncated Parquet trailer"
        )));
    }
    if &trailer[4..] != b"PAR1" {
        return Err(Error(format!(
            "object {location} has no Parquet footer magic"
        )));
    }

    let metadata_size = u64::from(u32::from_le_bytes(trailer[..4].try_into().unwrap()));
    let metadata_start = size
        .checked_sub(PARQUET_FOOTER_SIZE + metadata_size)
        .ok_or_else(|| {
            Error(format!(
                "object {location} has an invalid Parquet footer size"
            ))
        })?;
    let mut footer = store
        .get_range(location, metadata_start..size - PARQUET_FOOTER_SIZE)
        .await
        .map_err(object_store_error)?
        .to_vec();
    if footer.len() != metadata_size as usize {
        return Err(Error(format!(
            "object {location} returned truncated Parquet metadata"
        )));
    }
    footer.extend_from_slice(&trailer);
    Ok((size, read_footer_masses(&footer)?))
}

fn object_store_error(error: object_store::Error) -> Error {
    Error(format!("object store: {error}"))
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::ops::Range;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        path::Path, CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, Result,
    };

    use super::read_object_store_masses;
    use crate::parquet_helpers::{default_metadata_parser, MetadataParser};

    #[derive(Debug, Clone)]
    struct RecordingStore {
        inner: Arc<InMemory>,
        requests: Arc<Mutex<Vec<GetOptions>>>,
    }

    impl fmt::Display for RecordingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "recording")
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.requests.lock().unwrap().push(options.clone());
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn reads_only_parquet_footer_ranges() {
        let source = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_snappy.parquet"
        ));
        let bytes = std::fs::read(source).unwrap();
        let size = bytes.len() as u64;
        let expected = default_metadata_parser().read_masses(source).unwrap();
        let store = RecordingStore {
            inner: Arc::new(InMemory::new()),
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let location = Path::from("table/part-000.parquet");
        store.put(&location, bytes.clone().into()).await.unwrap();

        let (actual_size, actual) = read_object_store_masses(&store, &location).await.unwrap();

        assert_eq!(actual_size, size);
        assert_eq!(actual.num_rows, expected.num_rows);
        assert_eq!(actual.columns.len(), expected.columns.len());
        let footer_size = u64::from(u32::from_le_bytes(
            bytes[size as usize - 8..size as usize - 4]
                .try_into()
                .unwrap(),
        ));
        let ranges: Vec<Range<u64>> = store
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|request| match &request.range {
                Some(GetRange::Bounded(range)) => Some(range.clone()),
                _ => None,
            })
            .collect();
        assert!(store
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.head));
        assert_eq!(
            ranges,
            vec![size - 8..size, size - 8 - footer_size..size - 8]
        );
        assert!(ranges.iter().all(|range| range.end - range.start < size));
    }
}

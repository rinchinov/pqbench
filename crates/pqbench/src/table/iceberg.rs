//! Iceberg snapshot storage analysis.
//!
//! The command is one function: [`iceberg`] takes an [`IcebergRequest`] and
//! returns the [`TableReport`]. Rendering is a fold of that report:
//! [`render_text`] prints the summary and byte-mass table, [`render_json`]
//! serializes it, and [`render_html`] wraps the byte-mass hierarchy in a
//! self-contained treemap.
//!
//! The metadata JSON names the snapshot; Avro manifests name the active files.
//! Only data-file footers are inspected. Delete files are counted and not
//! applied. Results measure physical storage, not decoded values or logical
//! live rows.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::path::{Component, Path, PathBuf};

use apache_avro::{from_value, Reader};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::bytemass::{self, aggregate, BytemassRequest, ColumnMassSummary, MassRow, MassSummary};
use crate::object_store;

/// Errors resolving a snapshot or measuring its active files.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "iceberg: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// A column's physical storage summed across all active data files.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct ColumnReport {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub path: String,
    /// Total on-disk bytes across the snapshot's active files.
    pub compressed_bytes: u64,
    /// Total encoded bytes before compression.
    pub uncompressed_bytes: u64,
    /// Compression codecs present in the active files.
    pub codecs: BTreeSet<String>,
}

/// A complete measurement of one Iceberg snapshot. File bytes include Parquet
/// overhead, but exclude metadata, manifests, and delete files.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct TableReport {
    /// Resolved snapshot id; `None` when the table has no current snapshot.
    pub snapshot_id: Option<i64>,
    /// Number of active Parquet data files.
    pub file_count: usize,
    /// Total physical rows in the active data files.
    pub physical_rows: u64,
    /// Total size of the active Parquet data files, including file overhead.
    pub file_bytes: u64,
    /// Total compressed column-chunk bytes.
    pub compressed_column_bytes: u64,
    /// Total uncompressed column-chunk bytes.
    pub uncompressed_column_bytes: u64,
    /// Distinct delete files named by the snapshot's manifests.
    pub delete_file_count: usize,
    /// Distinct position-delete files among [`Self::delete_file_count`].
    pub position_delete_file_count: usize,
    /// Distinct equality-delete files among [`Self::delete_file_count`].
    pub equality_delete_file_count: usize,
    /// Per-column physical storage totals.
    pub columns: Vec<ColumnReport>,
    /// The measured table the report was folded from; kept for re-rendering.
    #[serde(skip)]
    rows: Vec<MassRow>,
}

impl TableReport {
    /// Total compressed column bytes per physical Parquet row.
    pub fn compressed_bytes_per_row(&self) -> f64 {
        if self.physical_rows == 0 {
            0.0
        } else {
            self.compressed_column_bytes as f64 / self.physical_rows as f64
        }
    }
}

/// Arguments for the `iceberg` command.
#[derive(Debug, Clone)]
pub struct IcebergRequest {
    /// Metadata JSON path or URI (`file://`, `s3://`, ...).
    pub metadata: String,
    /// Snapshot id; `None` selects the current snapshot.
    pub snapshot_id: Option<i64>,
}

/// Analyze the current or requested snapshot of an Iceberg table.
///
/// `request.metadata` is a filesystem path or a storage URI of the table's
/// metadata JSON. Bare paths and `file://` URIs are read through the
/// filesystem; other schemes use `pqbench::object_store` (S3 requires the
/// `aws` feature). Run inside a Tokio runtime.
///
/// # Errors
/// Fails for invalid metadata, missing snapshots, missing or changed active
/// files, non-Parquet data files, or data paths outside the table location.
/// No partial report is returned on failure.
pub async fn iceberg(request: &IcebergRequest) -> Result<TableReport, Error> {
    let metadata_bytes = read_location(&request.metadata).await?;
    let metadata: TableMetadata = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| Error(format!("cannot parse metadata JSON: {e}")))?;
    if metadata.format_version != 1 && metadata.format_version != 2 {
        return Err(Error(format!(
            "unsupported Iceberg format version: {}",
            metadata.format_version
        )));
    }
    let selected = select_snapshot(&metadata, request.snapshot_id)?;
    let snapshot_id = selected.as_ref().map(|snapshot| snapshot.snapshot_id);
    let files = match selected {
        Some(snapshot) => active_files(&metadata.location, &snapshot.manifest_list).await?,
        None => MeasuredFiles::default(),
    };
    let (rows, file_bytes) = measure_active(&files.data).await?;
    build_report(snapshot_id, file_bytes, rows, &files)
}

#[derive(Debug, Deserialize)]
struct TableMetadata {
    #[serde(rename = "format-version")]
    format_version: i32,
    location: String,
    #[serde(rename = "current-snapshot-id", default)]
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "manifest-list")]
    manifest_list: String,
}

#[derive(Debug, Deserialize)]
struct ManifestFile {
    manifest_path: String,
    #[serde(default)]
    content: i32,
}

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    status: i32,
    data_file: DataFile,
}

#[derive(Debug, Deserialize)]
struct DataFile {
    #[serde(default)]
    content: i32,
    file_path: String,
    file_format: String,
    file_size_in_bytes: i64,
}

#[derive(Default)]
struct MeasuredFiles {
    data: Vec<ActiveFile>,
    delete_files: BTreeSet<String>,
    position_delete_files: BTreeSet<String>,
    equality_delete_files: BTreeSet<String>,
}

struct ActiveFile {
    input: String,
    location: String,
    expected: u64,
}

const STATUS_DELETED: i32 = 2;
const CONTENT_DATA: i32 = 0;
const CONTENT_POSITION_DELETES: i32 = 1;
const CONTENT_EQUALITY_DELETES: i32 = 2;
const MANIFEST_DELETES: i32 = 1;

fn select_snapshot(
    metadata: &TableMetadata,
    requested: Option<i64>,
) -> Result<Option<&Snapshot>, Error> {
    let snapshot_id = match requested {
        Some(snapshot_id) => snapshot_id,
        None => match metadata.current_snapshot_id.filter(|id| *id != -1) {
            Some(snapshot_id) => snapshot_id,
            None => return Ok(None),
        },
    };
    metadata
        .snapshots
        .iter()
        .find(|snapshot| snapshot.snapshot_id == snapshot_id)
        .map(Some)
        .ok_or_else(|| Error(format!("Iceberg snapshot does not exist: {snapshot_id}")))
}

async fn active_files(table_location: &str, manifest_list: &str) -> Result<MeasuredFiles, Error> {
    let root = table_root(table_location)?;
    let bytes = read_location(manifest_list).await?;
    let manifests: Vec<ManifestFile> = read_avro(&bytes, "manifest list")?;
    let mut files = MeasuredFiles::default();
    for manifest in manifests {
        collect_manifest(&root, &manifest, &mut files).await?;
    }
    Ok(files)
}

async fn collect_manifest(
    root: &TableRoot,
    manifest: &ManifestFile,
    files: &mut MeasuredFiles,
) -> Result<(), Error> {
    let bytes = read_location(&manifest.manifest_path).await?;
    let entries: Vec<ManifestEntry> = read_avro(&bytes, "manifest")?;
    for entry in entries {
        if entry.status == STATUS_DELETED {
            continue;
        }
        let content = if manifest.content == MANIFEST_DELETES && entry.data_file.content == 0 {
            CONTENT_POSITION_DELETES
        } else {
            entry.data_file.content
        };
        if content != CONTENT_DATA {
            record_delete(&entry.data_file.file_path, content, files);
            continue;
        }
        if !entry.data_file.file_format.eq_ignore_ascii_case("parquet") {
            return Err(Error(format!(
                "active data file is not Parquet: {}",
                entry.data_file.file_path
            )));
        }
        let expected = u64::try_from(entry.data_file.file_size_in_bytes).map_err(|_| {
            Error(format!(
                "invalid file size in manifest: {}",
                entry.data_file.file_path
            ))
        })?;
        let input = resolve_data_file(root, &entry.data_file.file_path, expected)?;
        files.data.push(ActiveFile {
            input,
            location: entry.data_file.file_path,
            expected,
        });
    }
    Ok(())
}

fn record_delete(path: &str, content: i32, files: &mut MeasuredFiles) {
    files.delete_files.insert(path.to_string());
    match content {
        CONTENT_EQUALITY_DELETES => {
            files.equality_delete_files.insert(path.to_string());
        }
        _ => {
            files.position_delete_files.insert(path.to_string());
        }
    }
}

fn read_avro<T: for<'de> Deserialize<'de>>(bytes: &[u8], kind: &str) -> Result<Vec<T>, Error> {
    let reader = Reader::new(Cursor::new(bytes))
        .map_err(|e| Error(format!("cannot read Iceberg {kind}: {e}")))?;
    reader
        .map(|value| {
            let value = value.map_err(|e| Error(format!("cannot read Iceberg {kind}: {e}")))?;
            from_value::<T>(&value).map_err(|e| Error(format!("cannot parse Iceberg {kind}: {e}")))
        })
        .collect()
}

enum TableRoot {
    Local(PathBuf),
    Object(Url),
}

fn table_root(location: &str) -> Result<TableRoot, Error> {
    if looks_like_uri(location) {
        let url = parse_dir_url(location)?;
        if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|()| {
                    Error(format!(
                        "cannot convert table location to a path: {location}"
                    ))
                })?
                .canonicalize()
                .map_err(|e| Error(format!("cannot open table location {location}: {e}")))?;
            return Ok(TableRoot::Local(path));
        }
        return Ok(TableRoot::Object(url));
    }
    let path = Path::new(location)
        .canonicalize()
        .map_err(|e| Error(format!("cannot open table location {location}: {e}")))?;
    Ok(TableRoot::Local(path))
}

fn resolve_data_file(root: &TableRoot, location: &str, expected: u64) -> Result<String, Error> {
    match root {
        TableRoot::Local(root) => local_input(root, location, expected),
        TableRoot::Object(base) => object_input(base, location),
    }
}

fn local_input(root: &Path, location: &str, expected: u64) -> Result<String, Error> {
    let path = local_data_path(root, location)?;
    let size = std::fs::metadata(&path)
        .map_err(|e| Error(format!("cannot open active file {location}: {e}")))?
        .len();
    if size != expected {
        return Err(Error(format!(
            "active file size differs from manifest: {location} (expected {expected}, found {size})"
        )));
    }
    Ok(path.to_string_lossy().into_owned())
}

fn local_data_path(root: &Path, location: &str) -> Result<PathBuf, Error> {
    let candidate = if looks_like_uri(location) {
        let url = Url::parse(location)
            .map_err(|e| Error(format!("active data file is not a URI: {location}: {e}")))?;
        if url.scheme() != "file" {
            return Err(Error(format!("active data file is not local: {location}")));
        }
        url.to_file_path().map_err(|()| {
            Error(format!(
                "cannot convert active data file to a path: {location}"
            ))
        })?
    } else {
        relative_data_path(location)?.to_path_buf()
    };
    let local = if candidate.is_absolute() {
        candidate
    } else {
        root.join(candidate)
    }
    .canonicalize()
    .map_err(|e| Error(format!("cannot open active file {location}: {e}")))?;
    if !local.starts_with(root) {
        return Err(Error(format!(
            "active data file is outside the Iceberg table location: {location}"
        )));
    }
    Ok(local)
}

fn object_input(base: &Url, location: &str) -> Result<String, Error> {
    let uri = if looks_like_uri(location) {
        location.to_string()
    } else {
        relative_data_path(location)?;
        base.join(location)
            .map_err(|e| Error(format!("invalid active file path {location}: {e}")))?
            .into()
    };
    if !uri_inside(base, &uri) {
        return Err(Error(format!(
            "active data file is outside the Iceberg table location: {location}"
        )));
    }
    Ok(uri)
}

fn uri_inside(base: &Url, location: &str) -> bool {
    let Ok(url) = Url::parse(location) else {
        return false;
    };
    url.scheme() == base.scheme()
        && url.host_str() == base.host_str()
        && url.path().starts_with(base.path())
}

fn relative_data_path(relative: &str) -> Result<&Path, Error> {
    let path = Path::new(relative);
    if relative.is_empty()
        || relative.contains("://")
        || !path.components().all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(Error(format!(
            "only relative data paths inside the table are supported: {relative}"
        )));
    }
    Ok(path)
}

fn parse_dir_url(location: &str) -> Result<Url, Error> {
    let mut url = Url::parse(location)
        .map_err(|e| Error(format!("invalid table location {location}: {e}")))?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn looks_like_uri(value: &str) -> bool {
    value.contains("://")
}

async fn read_location(location: &str) -> Result<Vec<u8>, Error> {
    if looks_like_uri(location) {
        let reader = object_store::open(location, &[]).map_err(|e| Error(e.to_string()))?;
        let stat = reader.stat().await.map_err(|e| Error(e.to_string()))?;
        reader
            .read_range(0..stat.size, stat.identity.as_deref())
            .await
            .map_err(|e| Error(e.to_string()))
    } else {
        std::fs::read(location).map_err(|e| Error(format!("cannot read {location}: {e}")))
    }
}

async fn measure_active(active: &[ActiveFile]) -> Result<(Vec<MassRow>, u64), Error> {
    if active.is_empty() {
        return Ok((vec![], 0));
    }
    let request = BytemassRequest {
        inputs: active.iter().map(|file| file.input.clone()).collect(),
    };
    let rows = bytemass::bytemass(&request)
        .await
        .map_err(|e| Error(format!("cannot read active files: {e}")))?;
    let file_bytes = verify_sizes(active, &rows)?;
    Ok((rows, file_bytes))
}

fn verify_sizes(active: &[ActiveFile], rows: &[MassRow]) -> Result<u64, Error> {
    for row in rows {
        let file = active
            .iter()
            .find(|file| file.input == row.file)
            .ok_or_else(|| Error(format!("unexpected measured file: {}", row.file)))?;
        if row.size != file.expected {
            return Err(Error(format!(
                "active file size differs from manifest: {} (expected {}, found {})",
                file.location, file.expected, row.size
            )));
        }
    }
    let mut file_bytes = 0;
    for file in active {
        file_bytes = checked_sum(file_bytes, file.expected)?;
    }
    Ok(file_bytes)
}

fn build_report(
    snapshot_id: Option<i64>,
    file_bytes: u64,
    rows: Vec<MassRow>,
    files: &MeasuredFiles,
) -> Result<TableReport, Error> {
    let summary = aggregate(&rows).map_err(|e| Error(e.to_string()))?;
    let MassSummary {
        file_count,
        num_rows,
        columns,
    } = summary;
    let columns = into_report_columns(columns);
    let mut compressed_column_bytes = 0;
    let mut uncompressed_column_bytes = 0;
    for column in &columns {
        compressed_column_bytes = checked_sum(compressed_column_bytes, column.compressed_bytes)?;
        uncompressed_column_bytes =
            checked_sum(uncompressed_column_bytes, column.uncompressed_bytes)?;
    }
    Ok(TableReport {
        snapshot_id,
        file_count,
        physical_rows: num_rows,
        file_bytes,
        compressed_column_bytes,
        uncompressed_column_bytes,
        delete_file_count: files.delete_files.len(),
        position_delete_file_count: files.position_delete_files.len(),
        equality_delete_file_count: files.equality_delete_files.len(),
        columns,
        rows,
    })
}

fn into_report_columns(columns: Vec<ColumnMassSummary>) -> Vec<ColumnReport> {
    columns
        .into_iter()
        .map(|column| ColumnReport {
            path: column.path,
            compressed_bytes: column.compressed_bytes,
            uncompressed_bytes: column.uncompressed_bytes,
            codecs: column.codecs,
        })
        .collect()
}

fn checked_sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("storage totals exceed u64".into()))
}

/// Serialize the snapshot report as pretty-printed JSON.
///
/// # Errors
/// Returns an error if the report cannot be serialized.
pub fn render_json(report: &TableReport) -> Result<String, Error> {
    serde_json::to_string_pretty(report).map_err(|e| Error(format!("cannot serialize report: {e}")))
}

/// Render the physical byte-mass hierarchy as a self-contained HTML treemap.
///
/// # Errors
/// Returns an error if the hierarchy cannot be aggregated or serialized.
pub fn render_html(report: &TableReport) -> Result<String, Error> {
    bytemass::render_html(&report.rows).map_err(|e| Error(e.to_string()))
}

/// Render the snapshot summary followed by the byte-mass table.
///
/// # Errors
/// Returns an error if a byte total overflows while aggregating.
pub fn render_text(report: &TableReport) -> Result<String, Error> {
    let table = bytemass::render_text(&report.rows).map_err(|e| Error(e.to_string()))?;
    Ok(format!(
        "iceberg snapshot: {}\nactive data files: {}\nphysical rows: {}\nactive parquet bytes: {}\ncompressed column bytes: {}\nuncompressed column bytes: {}\ndelete files: {} (position: {}, equality: {}, not applied)\n{}",
        report
            .snapshot_id
            .map_or_else(|| "none".into(), |id| id.to_string()),
        report.file_count,
        report.physical_rows,
        report.file_bytes,
        report.compressed_column_bytes,
        report.uncompressed_column_bytes,
        report.delete_file_count,
        report.position_delete_file_count,
        report.equality_delete_file_count,
        table,
    ))
}

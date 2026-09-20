//! DuckLake snapshot storage analysis.
//!
//! The command is one function: [`ducklake`] takes a [`DuckLakeRequest`] and
//! returns the [`TableReport`]. Rendering is a fold of that report:
//! [`render_text`] prints the summary and byte-mass table, [`render_json`]
//! serializes it, and [`render_html`] wraps the byte-mass hierarchy in a
//! self-contained treemap.
//!
//! The metadata catalog is a local SQLite database. Active data files are
//! then measured through `bytemass` (local paths or object URIs). Delete
//! files are counted and not applied. Results measure physical storage, not
//! decoded values or logical live rows.

use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use url::Url;

use crate::bytemass::{self, aggregate, BytemassRequest, MassRow, MassSummary};

/// Errors resolving a snapshot or measuring its active files.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ducklake: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// One delete file active for the selected table snapshot.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct DeleteFileReport {
    /// Path as recorded in the catalog.
    pub path: String,
    /// Delete-file format recorded in the catalog.
    pub format: String,
    /// Size recorded in the catalog and confirmed on storage.
    pub file_bytes: u64,
    /// Deleted-row count recorded in the catalog.
    pub deleted_rows: u64,
}

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

/// A complete measurement of one DuckLake table snapshot.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct TableReport {
    /// Resolved snapshot id.
    pub snapshot: u64,
    /// Schema name selected from the catalog.
    pub schema: String,
    /// Table name selected from the catalog.
    pub table: String,
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
    /// Number of active delete files.
    pub delete_file_count: usize,
    /// Total size of the active delete files.
    pub delete_file_bytes: u64,
    /// Deleted-row count summed from the catalog.
    pub deleted_rows: u64,
    /// Active delete files. They are not applied to the byte-mass totals.
    pub delete_files: Vec<DeleteFileReport>,
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

/// Arguments for the `ducklake` command.
#[derive(Debug, Clone)]
pub struct DuckLakeRequest {
    /// Local DuckLake metadata catalog (SQLite).
    pub catalog: String,
    /// Schema name; DuckLake defaults to `main`.
    pub schema: String,
    /// Table name inside [`Self::schema`].
    pub table: String,
    /// Snapshot id; `None` selects the latest.
    pub snapshot: Option<u64>,
}

/// Analyze the latest or requested snapshot of a DuckLake table.
///
/// `request.catalog` is a local SQLite metadata database. The data path comes
/// from DuckLake metadata and may be a local directory or an object URI
/// (`s3://` requires the `aws` feature). Run inside a Tokio runtime.
///
/// # Errors
/// Fails for missing metadata, invalid snapshot or table names, inline or
/// encrypted data, unsupported formats, missing or changed files, or path
/// escapes. No partial report is returned on failure.
pub async fn ducklake(request: &DuckLakeRequest) -> Result<TableReport, Error> {
    let selected = select_snapshot(request)?;
    let measured = measure_files(&selected).await?;
    build_report(selected, measured)
}

struct SnapshotFiles {
    snapshot: u64,
    schema: String,
    table: String,
    table_location: String,
    root: DataRoot,
    data_files: Vec<DataFile>,
    delete_files: Vec<DeleteFile>,
}

#[derive(Clone)]
enum DataRoot {
    Local(PathBuf),
    Object(Url),
}

struct DataFile {
    id: i64,
    path: String,
    path_is_relative: bool,
    format: String,
    record_count: u64,
    file_size_bytes: u64,
    encrypted: bool,
}

struct DeleteFile {
    data_file_id: i64,
    path: String,
    path_is_relative: bool,
    format: String,
    delete_count: u64,
    file_size_bytes: u64,
    encrypted: bool,
}

struct MeasuredFiles {
    file_bytes: u64,
    rows: Vec<MassRow>,
    delete_file_bytes: u64,
    deleted_rows: u64,
    delete_files: Vec<DeleteFileReport>,
}

fn select_snapshot(request: &DuckLakeRequest) -> Result<SnapshotFiles, Error> {
    let catalog = Path::new(&request.catalog).canonicalize().map_err(|e| {
        Error(format!(
            "cannot open metadata catalog {}: {e}",
            request.catalog
        ))
    })?;
    if !catalog.is_file() {
        return Err(Error(format!(
            "metadata catalog is not a file: {}",
            catalog.display()
        )));
    }
    let connection =
        Connection::open(&catalog).map_err(|e| db_error("open metadata catalog", e))?;
    connection
        .execute_batch("BEGIN TRANSACTION")
        .map_err(|e| db_error("begin metadata transaction", e))?;
    let result = read_in_transaction(&connection, &catalog, request);
    let end = connection.execute_batch("COMMIT");
    result.and_then(|files| {
        end.map_err(|e| db_error("commit metadata transaction", e))?;
        Ok(files)
    })
}

fn read_in_transaction(
    connection: &Connection,
    catalog: &Path,
    request: &DuckLakeRequest,
) -> Result<SnapshotFiles, Error> {
    validate_format_version(connection)?;
    let root = data_root(connection, catalog)?;
    load_snapshot(connection, &root, request)
}

fn validate_format_version(connection: &Connection) -> Result<(), Error> {
    let version = global_metadata(connection, "version")?;
    let version = version.trim();
    if version == "0.3" || version.starts_with("1.") {
        Ok(())
    } else {
        Err(Error(format!(
            "unsupported DuckLake format version: {version} (expected 0.3 or 1.x)"
        )))
    }
}

fn data_root(connection: &Connection, catalog: &Path) -> Result<DataRoot, Error> {
    let path = global_metadata(connection, "data_path")?;
    if looks_like_uri(&path) {
        let url = parse_dir_url(&path)?;
        if url.scheme() == "file" {
            let local = url
                .to_file_path()
                .map_err(|()| Error(format!("cannot convert data_path to a path: {path}")))?;
            return local_data_root(&local, &path);
        }
        if url.scheme() != "s3" && url.scheme() != "s3a" {
            return Err(Error(format!(
                "unsupported DuckLake data_path scheme: {path}"
            )));
        }
        return Ok(DataRoot::Object(url));
    }
    reject_scheme(&path, "data_path")?;
    let raw = Path::new(&path);
    let candidate = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        catalog.parent().unwrap_or_else(|| Path::new(".")).join(raw)
    };
    local_data_root(&candidate, &path)
}

fn local_data_root(candidate: &Path, path: &str) -> Result<DataRoot, Error> {
    let root = candidate
        .canonicalize()
        .map_err(|e| Error(format!("cannot open DuckLake data_path {path}: {e}")))?;
    if !root.is_dir() {
        return Err(Error(format!(
            "DuckLake data_path is not a directory: {path}"
        )));
    }
    Ok(DataRoot::Local(root))
}

fn load_snapshot(
    connection: &Connection,
    root: &DataRoot,
    request: &DuckLakeRequest,
) -> Result<SnapshotFiles, Error> {
    let snapshot = match request.snapshot {
        Some(snapshot) => i64::try_from(snapshot)
            .map_err(|_| Error(format!("snapshot id is too large: {snapshot}")))?,
        None => connection
            .query_row(
                "SELECT max(snapshot_id) FROM ducklake_snapshot",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(|e| db_error("resolve latest DuckLake snapshot", e))?
            .ok_or_else(|| Error("DuckLake catalog has no snapshots".into()))?,
    };
    let snapshot_u64 = u64::try_from(snapshot)
        .map_err(|_| Error(format!("DuckLake snapshot id is negative: {snapshot}")))?;
    let exists: Option<i64> = connection
        .query_row(
            "SELECT snapshot_id FROM ducklake_snapshot WHERE snapshot_id = ?",
            params![snapshot],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| db_error("validate DuckLake snapshot", e))?;
    if exists.is_none() {
        return Err(Error(format!(
            "DuckLake snapshot does not exist: {snapshot}"
        )));
    }

    let schema_row: Option<(i64, Option<String>, i64)> = connection
        .query_row(
            "SELECT schema_id, path, path_is_relative FROM ducklake_schema
             WHERE schema_name = ? AND ? >= begin_snapshot
               AND (? < end_snapshot OR end_snapshot IS NULL)
             LIMIT 1",
            params![request.schema, snapshot, snapshot],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|e| db_error("select DuckLake schema", e))?;
    let (schema_id, schema_path, schema_relative) = schema_row.ok_or_else(|| {
        Error(format!(
            "DuckLake schema is not active at snapshot {snapshot}: {}",
            request.schema
        ))
    })?;
    let schema_base = declared_location(
        root,
        &root_location(root),
        schema_path.as_deref(),
        schema_relative != 0,
        "schema",
    )?;

    let table_row: Option<(i64, Option<String>, i64)> = connection
        .query_row(
            "SELECT table_id, path, path_is_relative FROM ducklake_table
             WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot
               AND (? < end_snapshot OR end_snapshot IS NULL)
             LIMIT 1",
            params![schema_id, request.table, snapshot, snapshot],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|e| db_error("select DuckLake table", e))?;
    let (table_id, table_path, table_relative) = table_row.ok_or_else(|| {
        Error(format!(
            "DuckLake table is not active at snapshot {snapshot}: {}.{}",
            request.schema, request.table
        ))
    })?;
    let table_location = declared_location(
        root,
        &schema_base,
        table_path.as_deref(),
        table_relative != 0,
        "table",
    )?;

    let inlined: Option<String> = connection
        .query_row(
            "SELECT table_name FROM ducklake_inlined_data_tables WHERE table_id = ? LIMIT 1",
            params![table_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| db_error("check DuckLake inlined data", e))?;
    if let Some(name) = inlined {
        return Err(Error(format!(
            "DuckLake table uses inlined data, which is unsupported: {}.{} ({name})",
            request.schema, request.table
        )));
    }

    let encrypted = global_metadata(connection, "encrypted")?;
    if encrypted.eq_ignore_ascii_case("true") {
        return Err(Error("encrypted DuckLake data is unsupported".into()));
    }

    let mut statement = connection
        .prepare(
            "SELECT data_file_id, path, path_is_relative, file_format, record_count,
                    file_size_bytes, encryption_key IS NOT NULL
             FROM ducklake_data_file
             WHERE table_id = ? AND ? >= begin_snapshot
               AND (? < end_snapshot OR end_snapshot IS NULL)
             ORDER BY file_order, data_file_id",
        )
        .map_err(|e| db_error("list active DuckLake data files", e))?;
    let data_files: Vec<DataFile> = statement
        .query_map(params![table_id, snapshot, snapshot], |row| {
            Ok(DataFile {
                id: row.get(0)?,
                path: row.get(1)?,
                path_is_relative: row.get::<_, i64>(2)? != 0,
                format: row.get::<_, Option<String>>(3)?.ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "null file format",
                        )),
                    )
                })?,
                record_count: nonnegative(row.get(4)?, "record_count")?,
                file_size_bytes: nonnegative(row.get(5)?, "file_size_bytes")?,
                encrypted: row.get(6)?,
            })
        })
        .and_then(Iterator::collect)
        .map_err(|e| db_error("list active DuckLake data files", e))?;

    let mut statement = connection
        .prepare(
            "SELECT data_file_id, path, path_is_relative, format, delete_count,
                    file_size_bytes, encryption_key IS NOT NULL
             FROM ducklake_delete_file
             WHERE table_id = ? AND ? >= begin_snapshot
               AND (? < end_snapshot OR end_snapshot IS NULL)
             ORDER BY data_file_id, delete_file_id",
        )
        .map_err(|e| db_error("list active DuckLake delete files", e))?;
    let delete_files: Vec<DeleteFile> = statement
        .query_map(params![table_id, snapshot, snapshot], |row| {
            Ok(DeleteFile {
                data_file_id: row.get(0)?,
                path: row.get(1)?,
                path_is_relative: row.get::<_, i64>(2)? != 0,
                format: row
                    .get::<_, Option<String>>(3)?
                    .unwrap_or_else(|| "unknown".into()),
                delete_count: nonnegative(row.get(4)?, "delete_count")?,
                file_size_bytes: nonnegative(row.get(5)?, "file_size_bytes")?,
                encrypted: row.get(6)?,
            })
        })
        .and_then(Iterator::collect)
        .map_err(|e| db_error("list active DuckLake delete files", e))?;

    let active_ids: HashSet<i64> = data_files.iter().map(|file| file.id).collect();
    for file in &delete_files {
        if !active_ids.contains(&file.data_file_id) {
            return Err(Error(format!(
                "active delete file references an inactive data file: {}",
                file.data_file_id
            )));
        }
    }

    Ok(SnapshotFiles {
        snapshot: snapshot_u64,
        schema: request.schema.clone(),
        table: request.table.clone(),
        table_location,
        root: root.clone(),
        data_files,
        delete_files,
    })
}

fn global_metadata(connection: &Connection, key: &str) -> Result<String, Error> {
    connection
        .query_row(
            "SELECT value FROM ducklake_metadata
             WHERE key = ? AND scope IS NULL AND scope_id IS NULL LIMIT 1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| db_error("read DuckLake metadata", e))?
        .ok_or_else(|| Error(format!("DuckLake metadata has no global {key} value")))
}

fn root_location(root: &DataRoot) -> String {
    match root {
        DataRoot::Local(path) => path.to_string_lossy().into_owned(),
        DataRoot::Object(url) => url.to_string(),
    }
}

fn declared_location(
    root: &DataRoot,
    base: &str,
    path: Option<&str>,
    relative: bool,
    kind: &str,
) -> Result<String, Error> {
    let Some(path) = path else {
        return Ok(base.to_string());
    };
    join_location(root, base, path, relative, kind)
}

fn join_location(
    root: &DataRoot,
    base: &str,
    path: &str,
    relative: bool,
    kind: &str,
) -> Result<String, Error> {
    if relative {
        reject_scheme(path, kind)?;
        if !normal_components(Path::new(path)) {
            return Err(Error(format!(
                "invalid relative DuckLake {kind} path: {path}"
            )));
        }
        return match root {
            DataRoot::Local(_) => Ok(Path::new(base).join(path).to_string_lossy().into_owned()),
            DataRoot::Object(url) => {
                let mut directory = if looks_like_uri(base) {
                    parse_dir_url(base)?
                } else {
                    url.clone()
                };
                if !directory.path().ends_with('/') {
                    directory.set_path(&format!("{}/", directory.path()));
                }
                directory
                    .join(path)
                    .map(|joined| joined.into())
                    .map_err(|e| Error(format!("invalid DuckLake {kind} path {path}: {e}")))
            }
        };
    }
    if looks_like_uri(path) {
        let url =
            Url::parse(path).map_err(|e| Error(format!("invalid DuckLake {kind} path: {e}")))?;
        ensure_uri_contained(root, &url, kind)?;
        return Ok(url.into());
    }
    reject_scheme(path, kind)?;
    let candidate = Path::new(path);
    if candidate.exists() {
        let canonical = candidate.canonicalize().map_err(|e| {
            Error(format!(
                "cannot resolve DuckLake {kind} path {}: {e}",
                candidate.display()
            ))
        })?;
        ensure_local_contained(root, &canonical, kind)?;
        Ok(canonical.to_string_lossy().into_owned())
    } else {
        Ok(path.to_string())
    }
}

async fn measure_files(files: &SnapshotFiles) -> Result<MeasuredFiles, Error> {
    let mut active = Vec::with_capacity(files.data_files.len());
    for file in &files.data_files {
        if !file.format.eq_ignore_ascii_case("parquet") {
            return Err(Error(format!(
                "unsupported DuckLake data file format for {}: {}",
                file.path, file.format
            )));
        }
        if file.encrypted {
            return Err(Error(format!(
                "encrypted DuckLake data file is unsupported: {}",
                file.path
            )));
        }
        let input = resolve_file(
            &files.root,
            &files.table_location,
            &file.path,
            file.path_is_relative,
            file.file_size_bytes,
            "data file",
        )?;
        active.push(ActiveFile {
            input,
            path: file.path.clone(),
            expected: file.file_size_bytes,
            record_count: Some(file.record_count),
        });
    }

    let (rows, file_bytes) = measure_active(&active).await?;
    for file in &files.data_files {
        if let Some(row) = rows.iter().find(|row| {
            active
                .iter()
                .any(|active| active.path == file.path && active.input == row.file)
        }) {
            if row.num_rows != file.record_count {
                return Err(Error(format!(
                    "active data file row count differs from metadata: {} (expected {}, found {})",
                    file.path, file.record_count, row.num_rows
                )));
            }
        }
    }

    let mut delete_file_bytes = 0;
    let mut deleted_rows = 0;
    let mut delete_reports = Vec::with_capacity(files.delete_files.len());
    for file in &files.delete_files {
        if file.encrypted {
            return Err(Error(format!(
                "encrypted DuckLake delete file is unsupported: {}",
                file.path
            )));
        }
        let input = resolve_file(
            &files.root,
            &files.table_location,
            &file.path,
            file.path_is_relative,
            file.file_size_bytes,
            "delete file",
        )?;
        delete_file_bytes = checked_sum(delete_file_bytes, file.file_size_bytes)?;
        deleted_rows = checked_sum(deleted_rows, file.delete_count)?;
        let _ = input;
        delete_reports.push(DeleteFileReport {
            path: file.path.clone(),
            format: file.format.clone(),
            file_bytes: file.file_size_bytes,
            deleted_rows: file.delete_count,
        });
    }
    Ok(MeasuredFiles {
        file_bytes,
        rows,
        delete_file_bytes,
        deleted_rows,
        delete_files: delete_reports,
    })
}

struct ActiveFile {
    input: String,
    path: String,
    expected: u64,
    record_count: Option<u64>,
}

fn resolve_file(
    root: &DataRoot,
    base: &str,
    path: &str,
    relative: bool,
    expected: u64,
    kind: &str,
) -> Result<String, Error> {
    let location = join_location(root, base, path, relative, kind)?;
    match root {
        DataRoot::Local(root) => {
            let local = Path::new(&location)
                .canonicalize()
                .map_err(|e| Error(format!("cannot open active {kind} {path}: {e}")))?;
            ensure_local_contained(&DataRoot::Local(root.clone()), &local, kind)?;
            let actual = std::fs::metadata(&local)
                .map_err(|e| Error(format!("cannot stat active {kind} {path}: {e}")))?
                .len();
            if actual != expected {
                return Err(Error(format!(
                    "active {kind} size differs from metadata: {path} (expected {expected}, found {actual})"
                )));
            }
            Ok(local.to_string_lossy().into_owned())
        }
        DataRoot::Object(_) => Ok(location),
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
    for row in &rows {
        let file = active
            .iter()
            .find(|file| file.input == row.file)
            .ok_or_else(|| Error(format!("unexpected measured file: {}", row.file)))?;
        if row.size != file.expected {
            return Err(Error(format!(
                "active data file size differs from metadata: {} (expected {}, found {})",
                file.path, file.expected, row.size
            )));
        }
        if let Some(expected_rows) = file.record_count {
            if row.num_rows != expected_rows {
                return Err(Error(format!(
                    "active data file row count differs from metadata: {} (expected {}, found {})",
                    file.path, expected_rows, row.num_rows
                )));
            }
        }
    }
    let mut file_bytes = 0;
    for file in active {
        file_bytes = checked_sum(file_bytes, file.expected)?;
    }
    Ok((rows, file_bytes))
}

fn build_report(files: SnapshotFiles, measured: MeasuredFiles) -> Result<TableReport, Error> {
    let summary = aggregate(&measured.rows).map_err(|e| Error(e.to_string()))?;
    let MassSummary {
        file_count,
        num_rows,
        columns,
    } = summary;
    let columns = columns
        .into_iter()
        .map(|column| ColumnReport {
            path: column.path,
            compressed_bytes: column.compressed_bytes,
            uncompressed_bytes: column.uncompressed_bytes,
            codecs: column.codecs,
        })
        .collect::<Vec<_>>();
    let mut compressed_column_bytes = 0;
    let mut uncompressed_column_bytes = 0;
    for column in &columns {
        compressed_column_bytes = checked_sum(compressed_column_bytes, column.compressed_bytes)?;
        uncompressed_column_bytes =
            checked_sum(uncompressed_column_bytes, column.uncompressed_bytes)?;
    }
    Ok(TableReport {
        snapshot: files.snapshot,
        schema: files.schema,
        table: files.table,
        file_count,
        physical_rows: num_rows,
        file_bytes: measured.file_bytes,
        compressed_column_bytes,
        uncompressed_column_bytes,
        delete_file_count: measured.delete_files.len(),
        delete_file_bytes: measured.delete_file_bytes,
        deleted_rows: measured.deleted_rows,
        delete_files: measured.delete_files,
        columns,
        rows: measured.rows,
    })
}

fn ensure_local_contained(root: &DataRoot, path: &Path, kind: &str) -> Result<(), Error> {
    let DataRoot::Local(root) = root else {
        return Err(Error(format!(
            "active {kind} is a local path but DuckLake data_path is remote: {}",
            path.display()
        )));
    };
    if !path.starts_with(root) {
        return Err(Error(format!(
            "active {kind} is outside the DuckLake data_path: {}",
            path.display()
        )));
    }
    Ok(())
}

fn ensure_uri_contained(root: &DataRoot, url: &Url, kind: &str) -> Result<(), Error> {
    let DataRoot::Object(base) = root else {
        return Err(Error(format!(
            "only local DuckLake {kind} paths are supported: {url}"
        )));
    };
    if url.scheme() != base.scheme()
        || url.host_str() != base.host_str()
        || !url.path().starts_with(base.path())
    {
        return Err(Error(format!(
            "active {kind} is outside the DuckLake data_path: {url}"
        )));
    }
    Ok(())
}

fn parse_dir_url(location: &str) -> Result<Url, Error> {
    let mut url = Url::parse(location)
        .map_err(|e| Error(format!("invalid DuckLake path {location}: {e}")))?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn reject_scheme(path: &str, kind: &str) -> Result<(), Error> {
    if looks_like_uri(path) {
        return Err(Error(format!(
            "only local DuckLake {kind} paths are supported: {path}"
        )));
    }
    Ok(())
}

fn looks_like_uri(path: &str) -> bool {
    path.contains("://")
}

fn normal_components(path: &Path) -> bool {
    path.components()
        .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn nonnegative(value: i64, name: &str) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("negative DuckLake {name}"),
            )),
        )
    })
}

fn db_error(context: &str, error: rusqlite::Error) -> Error {
    Error(format!("{context}: {error}"))
}

fn checked_sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("storage totals exceed u64".into()))
}

/// Serialize a DuckLake report as pretty-printed JSON.
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
        "DuckLake table: {}.{}\nsnapshot: {}\nactive data files: {}\nphysical rows: {}\nactive parquet bytes: {}\ncompressed column bytes: {}\nuncompressed column bytes: {}\nactive delete files: {}\ndelete-file bytes: {}\ndeleted rows: {}\n{}",
        report.schema,
        report.table,
        report.snapshot,
        report.file_count,
        report.physical_rows,
        report.file_bytes,
        report.compressed_column_bytes,
        report.uncompressed_column_bytes,
        report.delete_file_count,
        report.delete_file_bytes,
        report.deleted_rows,
        table,
    ))
}

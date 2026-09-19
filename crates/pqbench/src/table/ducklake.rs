//! Local DuckLake snapshot storage analysis.
//!
//! The metadata catalog is queried directly with DuckDB. Data files are then
//! checked on the local filesystem and their Parquet footers are measured by
//! [`crate::bytemass::MassAccumulator`]. No DuckLake extension is required.

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

use crate::bytemass::{self, MassAccumulator, MassNode, MassSummary};
use crate::parquet_helpers::{default_metadata_parser, MetadataParser};
use duckdb::{params, Connection, OptionalExt};
use serde::Serialize;

/// Errors resolving a local DuckLake snapshot or measuring its active files.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ducklake: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// One delete file active for the selected table snapshot.
#[derive(Debug, Serialize)]
pub struct DeleteFileReport {
    pub path: String,
    pub format: String,
    pub file_bytes: u64,
    pub deleted_rows: u64,
}

/// A column's physical storage summed across all active data files.
#[derive(Debug, Serialize)]
pub struct ColumnReport {
    pub path: String,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub codecs: std::collections::BTreeSet<String>,
}

/// A complete measurement of one local DuckLake table snapshot.
#[derive(Serialize)]
pub struct TableReport {
    pub snapshot: u64,
    pub schema: String,
    pub table: String,
    pub file_count: usize,
    pub physical_rows: u64,
    pub file_bytes: u64,
    pub compressed_column_bytes: u64,
    pub uncompressed_column_bytes: u64,
    pub delete_file_count: usize,
    pub delete_file_bytes: u64,
    pub deleted_rows: u64,
    pub delete_files: Vec<DeleteFileReport>,
    pub columns: Vec<ColumnReport>,
    /// Compressed column bytes per physical table row, for JSON/HTML consumers.
    tree: MassNode,
}

impl TableReport {
    /// Total compressed column bytes per physical Parquet row.
    pub fn compressed_bytes_per_row(&self) -> f64 {
        self.tree.value
    }
}

#[derive(Debug)]
struct SnapshotFiles {
    snapshot: u64,
    schema: String,
    table: String,
    table_path: PathBuf,
    data_files: Vec<DataFile>,
    delete_files: Vec<DeleteFile>,
}

#[derive(Debug)]
struct DataFile {
    id: i64,
    path: String,
    path_is_relative: bool,
    format: String,
    record_count: u64,
    file_size_bytes: u64,
    encrypted: bool,
}

#[derive(Debug)]
struct DeleteFile {
    data_file_id: i64,
    path: String,
    path_is_relative: bool,
    format: String,
    delete_count: u64,
    file_size_bytes: u64,
    encrypted: bool,
}

#[derive(Debug)]
struct MeasuredFiles {
    file_bytes: u64,
    mass: MassSummary,
    delete_file_bytes: u64,
    deleted_rows: u64,
    delete_files: Vec<DeleteFileReport>,
}

/// Analyze the latest or requested snapshot of a local DuckLake table.
///
/// `catalog` is the local DuckDB metadata database. The data path and the
/// schema/table path hierarchy are read from DuckLake metadata. `schema`
/// defaults to `main` in the CLI, while this library accepts it explicitly.
///
/// # Errors
/// Fails for remote paths, missing metadata, invalid snapshot/table names,
/// inline or encrypted data, unsupported data formats, missing/changed files,
/// path escapes, and Parquet footer errors. No partial report is returned.
pub fn read_local(
    catalog: &Path,
    schema: &str,
    table: &str,
    snapshot: Option<u64>,
) -> Result<TableReport, Error> {
    let catalog = catalog.canonicalize().map_err(|e| {
        Error(format!(
            "cannot open metadata catalog {}: {e}",
            catalog.display()
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
        .execute_batch("BEGIN TRANSACTION READ ONLY")
        .map_err(|e| db_error("begin read-only metadata transaction", e))?;
    let result = read_in_transaction(&connection, &catalog, schema, table, snapshot);
    let end_result = connection.execute_batch("COMMIT");
    if let Err(error) = end_result {
        return Err(db_error("commit metadata transaction", error));
    }
    result
}

fn read_in_transaction(
    connection: &Connection,
    catalog: &Path,
    schema: &str,
    table: &str,
    snapshot: Option<u64>,
) -> Result<TableReport, Error> {
    validate_format_version(connection)?;
    let root = data_root(&connection, &catalog)?;
    let selected = select_snapshot(&connection, &root, schema, table, snapshot)?;
    let measured = measure_files(&selected, &root)?;
    build_report(selected, measured)
}

fn validate_format_version(connection: &Connection) -> Result<(), Error> {
    let version = global_metadata(connection, "version")?;
    if version.trim() != "1.0" {
        return Err(Error(format!(
            "unsupported DuckLake format version: {version} (expected 1.0)"
        )));
    }
    Ok(())
}

fn data_root(connection: &Connection, catalog: &Path) -> Result<PathBuf, Error> {
    let path: Option<String> = connection
        .query_row(
            "SELECT value FROM ducklake_metadata
             WHERE key = 'data_path' AND scope IS NULL AND scope_id IS NULL
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| db_error("read DuckLake data_path", e))?;
    let path = path.ok_or_else(|| Error("DuckLake metadata has no global data_path".into()))?;
    let raw = Path::new(&path);
    reject_non_local_path(&path, "data_path")?;
    let candidate = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        catalog.parent().unwrap_or_else(|| Path::new(".")).join(raw)
    };
    let root = candidate
        .canonicalize()
        .map_err(|e| Error(format!("cannot open DuckLake data_path {path}: {e}")))?;
    if !root.is_dir() {
        return Err(Error(format!(
            "DuckLake data_path is not a directory: {path}"
        )));
    }
    Ok(root)
}

fn select_snapshot(
    connection: &Connection,
    root: &Path,
    schema: &str,
    table: &str,
    requested: Option<u64>,
) -> Result<SnapshotFiles, Error> {
    let snapshot = match requested {
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

    let schema_row: Option<(i64, Option<String>, bool)> = connection
        .query_row(
            "SELECT schema_id, path, path_is_relative FROM ducklake_schema
             WHERE schema_name = ? AND ? >= begin_snapshot
               AND (? < end_snapshot OR end_snapshot IS NULL)
             LIMIT 1",
            params![schema, snapshot, snapshot],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|e| db_error("select DuckLake schema", e))?;
    let (schema_id, schema_path, schema_relative) = schema_row.ok_or_else(|| {
        Error(format!(
            "DuckLake schema is not active at snapshot {snapshot}: {schema}"
        ))
    })?;
    let schema_base = declared_path(
        root,
        schema_path.as_deref(),
        schema_relative,
        root,
        "schema",
    )?;

    let table_row: Option<(i64, Option<String>, bool)> = connection
        .query_row(
            "SELECT table_id, path, path_is_relative FROM ducklake_table
             WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot
               AND (? < end_snapshot OR end_snapshot IS NULL)
             LIMIT 1",
            params![schema_id, table, snapshot, snapshot],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|e| db_error("select DuckLake table", e))?;
    let (table_id, table_path, table_relative) = table_row.ok_or_else(|| {
        Error(format!(
            "DuckLake table is not active at snapshot {snapshot}: {schema}.{table}"
        ))
    })?;
    let table_base = declared_path(
        &schema_base,
        table_path.as_deref(),
        table_relative,
        root,
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
            "DuckLake table uses inlined data, which is unsupported: {schema}.{table} ({name})"
        )));
    }

    let encrypted = global_metadata(connection, "encrypted")?;
    if encrypted.eq_ignore_ascii_case("true") {
        return Err(Error("encrypted DuckLake data is unsupported".into()));
    }

    let data_files: Vec<DataFile> = connection
        .prepare(
            "SELECT data_file_id, path, path_is_relative, file_format, record_count,
                    file_size_bytes, encryption_key IS NOT NULL
             FROM ducklake_data_file
             WHERE table_id = ? AND ? >= begin_snapshot
               AND (? < end_snapshot OR end_snapshot IS NULL)
             ORDER BY file_order, data_file_id",
        )
        .and_then(|mut statement| {
            statement
                .query_map(params![table_id, snapshot, snapshot], |row| {
                    Ok(DataFile {
                        id: row.get(0)?,
                        path: row.get(1)?,
                        path_is_relative: row.get(2)?,
                        format: row.get::<_, Option<String>>(3)?.ok_or_else(|| {
                            duckdb::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "null file format",
                            )))
                        })?,
                        record_count: nonnegative(row.get(4)?, "record_count")?,
                        file_size_bytes: nonnegative(row.get(5)?, "file_size_bytes")?,
                        encrypted: row.get(6)?,
                    })
                })
                .and_then(|rows| rows.collect())
        })
        .map_err(|e| db_error("list active DuckLake data files", e))?;

    let delete_files: Vec<DeleteFile> = connection
        .prepare(
            "SELECT data_file_id, path, path_is_relative, format, delete_count,
                    file_size_bytes, encryption_key IS NOT NULL
             FROM ducklake_delete_file
             WHERE table_id = ? AND ? >= begin_snapshot
               AND (? < end_snapshot OR end_snapshot IS NULL)
             ORDER BY data_file_id, delete_file_id",
        )
        .and_then(|mut statement| {
            statement
                .query_map(params![table_id, snapshot, snapshot], |row| {
                    Ok(DeleteFile {
                        data_file_id: row.get(0)?,
                        path: row.get(1)?,
                        path_is_relative: row.get(2)?,
                        format: row
                            .get::<_, Option<String>>(3)?
                            .unwrap_or_else(|| "unknown".into()),
                        delete_count: nonnegative(row.get(4)?, "delete_count")?,
                        file_size_bytes: nonnegative(row.get(5)?, "file_size_bytes")?,
                        encrypted: row.get(6)?,
                    })
                })
                .and_then(|rows| rows.collect())
        })
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
        schema: schema.into(),
        table: table.into(),
        table_path: table_base,
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

fn declared_path(
    base: &Path,
    path: Option<&str>,
    relative: bool,
    root: &Path,
    kind: &str,
) -> Result<PathBuf, Error> {
    let Some(path) = path else {
        return Ok(base.to_path_buf());
    };
    reject_non_local_path(path, kind)?;
    let path = Path::new(path);
    if relative && !normal_components(path) {
        return Err(Error(format!(
            "invalid relative DuckLake {kind} path: {}",
            path.display()
        )));
    }
    let candidate = if relative {
        base.join(path)
    } else {
        path.to_path_buf()
    };
    if candidate.exists() {
        let canonical = candidate.canonicalize().map_err(|e| {
            Error(format!(
                "cannot resolve DuckLake {kind} path {}: {e}",
                candidate.display()
            ))
        })?;
        ensure_contained(root, &canonical, kind)?;
        Ok(canonical)
    } else {
        Ok(candidate)
    }
}

fn measure_files(files: &SnapshotFiles, root: &Path) -> Result<MeasuredFiles, Error> {
    let parser = default_metadata_parser();
    let mut mass = MassAccumulator::new();
    let mut file_bytes = 0;
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
        let local = local_file(
            &files.table_path,
            &file.path,
            file.path_is_relative,
            root,
            "data file",
        )?;
        let actual = std::fs::metadata(&local)
            .map_err(|e| Error(format!("cannot stat active data file {}: {e}", file.path)))?
            .len();
        if actual != file.file_size_bytes {
            return Err(Error(format!(
                "active data file size differs from metadata: {} (expected {}, found {})",
                file.path, file.file_size_bytes, actual
            )));
        }
        let file_mass = parser
            .read_masses(&local)
            .map_err(|e| Error(format!("cannot read active data file {}: {e}", file.path)))?;
        if file_mass.num_rows != file.record_count {
            return Err(Error(format!(
                "active data file row count differs from metadata: {} (expected {}, found {})",
                file.path, file.record_count, file_mass.num_rows
            )));
        }
        file_bytes = checked_sum(file_bytes, actual)?;
        mass.add(file_mass).map_err(parquet_error)?;
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
        let local = local_file(
            &files.table_path,
            &file.path,
            file.path_is_relative,
            root,
            "delete file",
        )?;
        let actual = std::fs::metadata(&local)
            .map_err(|e| Error(format!("cannot stat active delete file {}: {e}", file.path)))?
            .len();
        if actual != file.file_size_bytes {
            return Err(Error(format!(
                "active delete file size differs from metadata: {} (expected {}, found {})",
                file.path, file.file_size_bytes, actual
            )));
        }
        delete_file_bytes = checked_sum(delete_file_bytes, actual)?;
        deleted_rows = checked_sum(deleted_rows, file.delete_count)?;
        delete_reports.push(DeleteFileReport {
            path: file.path.clone(),
            format: file.format.clone(),
            file_bytes: actual,
            deleted_rows: file.delete_count,
        });
    }
    Ok(MeasuredFiles {
        file_bytes,
        mass: mass.finish(),
        delete_file_bytes,
        deleted_rows,
        delete_files: delete_reports,
    })
}

fn build_report(files: SnapshotFiles, measured: MeasuredFiles) -> Result<TableReport, Error> {
    let mass = measured.mass.file_mass();
    let mut tree = bytemass::aggregate(&bytemass::read(&mass));
    tree.label = format!(
        "{}.{} @ snapshot {} (physical bytes/row)",
        files.schema, files.table, files.snapshot
    );
    let MassSummary {
        file_count,
        num_rows,
        columns,
    } = measured.mass;
    let columns: Vec<_> = columns
        .into_iter()
        .map(|column| ColumnReport {
            path: column.path,
            compressed_bytes: column.compressed_bytes,
            uncompressed_bytes: column.uncompressed_bytes,
            codecs: column.codecs,
        })
        .collect();
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
        tree,
    })
}

/// Serialize a DuckLake report as pretty-printed JSON.
pub fn json(report: &TableReport) -> Result<String, Error> {
    serde_json::to_string_pretty(report).map_err(|e| Error(format!("cannot serialize report: {e}")))
}

/// Render the physical byte-mass hierarchy as a self-contained HTML treemap.
pub fn render_html(report: &TableReport) -> Result<String, Error> {
    bytemass::render_html(&report.tree)
        .map_err(|e| Error(format!("cannot render HTML report: {e}")))
}

/// Render the snapshot summary followed by the existing byte-mass table.
pub fn render(report: &TableReport) -> String {
    format!(
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
        bytemass::render(&report.tree),
    )
}

fn local_file(
    base: &Path,
    path: &str,
    relative: bool,
    root: &Path,
    kind: &str,
) -> Result<PathBuf, Error> {
    reject_non_local_path(path, kind)?;
    let path = Path::new(path);
    if relative && !normal_components(path) {
        return Err(Error(format!(
            "invalid relative DuckLake {kind} path: {}",
            path.display()
        )));
    }
    let candidate = if relative {
        base.join(path)
    } else {
        path.to_path_buf()
    };
    let local = candidate.canonicalize().map_err(|e| {
        Error(format!(
            "cannot open active {kind} {}: {e}",
            candidate.display()
        ))
    })?;
    ensure_contained(root, &local, kind)?;
    Ok(local)
}

fn reject_non_local_path(path: &str, kind: &str) -> Result<(), Error> {
    let scheme = path.find(':').is_some_and(|colon| {
        path[..colon]
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    });
    if path.contains("://") || scheme {
        return Err(Error(format!(
            "only local DuckLake {kind} paths are supported: {path}"
        )));
    }
    Ok(())
}

fn normal_components(path: &Path) -> bool {
    path.components()
        .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn ensure_contained(root: &Path, path: &Path, kind: &str) -> Result<(), Error> {
    if !path.starts_with(root) {
        return Err(Error(format!(
            "active {kind} is outside the DuckLake data_path: {}",
            path.display()
        )));
    }
    Ok(())
}

fn nonnegative(value: i64, name: &str) -> duckdb::Result<u64> {
    u64::try_from(value).map_err(|_| {
        duckdb::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("negative DuckLake {name}"),
        )))
    })
}

fn db_error(context: &str, error: duckdb::Error) -> Error {
    Error(format!("{context}: {error}"))
}

fn parquet_error(error: crate::parquet_helpers::Error) -> Error {
    Error(error.to_string())
}

fn checked_sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("storage totals exceed u64".into()))
}

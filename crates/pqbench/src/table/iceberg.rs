//! Local Apache Iceberg snapshot storage analysis.
//!
//! The metadata JSON path is explicit. The current snapshot is measured by
//! default, or a specific snapshot can be selected. Only local Parquet data
//! files under the table location are accepted; delete files are counted and
//! reported, but are not applied to the Parquet byte-mass measurement.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use futures::TryStreamExt;
use iceberg::io::FileIOBuilder;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, DataFileFormat};
use iceberg::table::StaticTable;
use iceberg::TableIdent;
use serde::Serialize;
use url::Url;

use crate::bytemass::{self, MassAccumulator, MassNode, MassSummary};
use crate::parquet_helpers::{default_metadata_parser, MetadataParser};

/// Errors resolving a local Iceberg metadata file or measuring its snapshot.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "iceberg: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// One column's physical storage summed across all active data files.
#[derive(Serialize)]
pub struct ColumnReport {
    pub path: String,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub codecs: BTreeSet<String>,
}

/// A complete measurement of one local Iceberg snapshot.
#[derive(Serialize)]
pub struct TableReport {
    pub snapshot_id: Option<i64>,
    pub file_count: usize,
    pub physical_rows: u64,
    pub file_bytes: u64,
    pub compressed_column_bytes: u64,
    pub uncompressed_column_bytes: u64,
    pub delete_file_count: usize,
    pub position_delete_file_count: usize,
    pub equality_delete_file_count: usize,
    pub columns: Vec<ColumnReport>,
    /// Compressed column bytes per physical Parquet row, for JSON/HTML consumers.
    tree: MassNode,
}

impl TableReport {
    /// Total compressed column bytes per physical Parquet row.
    pub fn compressed_bytes_per_row(&self) -> f64 {
        self.tree.value
    }
}

/// Analyze the current or requested snapshot from a local metadata JSON file.
///
/// The metadata file must describe a local table. Active data files must be
/// local Parquet files contained by the table location, and their actual sizes
/// must match the sizes recorded in the Iceberg manifests.
pub async fn read_local(
    metadata_path: &Path,
    snapshot_id: Option<i64>,
) -> Result<TableReport, Error> {
    let metadata_path = metadata_path.canonicalize().map_err(|e| {
        Error(format!(
            "cannot open metadata file {}: {e}",
            metadata_path.display()
        ))
    })?;
    if !metadata_path.is_file() {
        return Err(Error(format!(
            "metadata path is not a file: {}",
            metadata_path.display()
        )));
    }

    let file_io = FileIOBuilder::new_fs_io().build().map_err(iceberg_error)?;
    let metadata_location = Url::from_file_path(&metadata_path)
        .map_err(|()| Error("cannot convert metadata path to a local file URL".into()))?
        .to_string();
    let table = StaticTable::from_metadata_file(
        &metadata_location,
        TableIdent::from_strs(["local", "pqbench"]).map_err(iceberg_error)?,
        file_io,
    )
    .await
    .map_err(iceberg_error)?;
    let root = local_table_root(table.metadata().location(), &metadata_path)?;

    let mut scan = table.scan();
    if let Some(snapshot_id) = snapshot_id {
        scan = scan.snapshot_id(snapshot_id);
    }
    let scan = scan.build().map_err(iceberg_error)?;
    let selected_snapshot_id = scan.snapshot().map(|snapshot| snapshot.snapshot_id());
    let mut tasks = scan.plan_files().await.map_err(iceberg_error)?;
    let measured = measure_tasks(&root, &mut tasks).await?;
    build_report(selected_snapshot_id, measured)
}

struct MeasuredFiles {
    file_bytes: u64,
    mass: MassSummary,
    delete_files: BTreeSet<String>,
    position_delete_files: BTreeSet<String>,
    equality_delete_files: BTreeSet<String>,
}

async fn measure_tasks(
    root: &Path,
    tasks: &mut iceberg::scan::FileScanTaskStream,
) -> Result<MeasuredFiles, Error> {
    let parser = default_metadata_parser();
    let mut mass = MassAccumulator::new();
    let mut file_bytes = 0;
    let mut delete_files = BTreeSet::new();
    let mut position_delete_files = BTreeSet::new();
    let mut equality_delete_files = BTreeSet::new();

    while let Some(task) = tasks.try_next().await.map_err(iceberg_error)? {
        collect_delete_files(
            &task,
            &mut delete_files,
            &mut position_delete_files,
            &mut equality_delete_files,
        );
        if task.data_file_format != DataFileFormat::Parquet {
            return Err(Error(format!(
                "active data file is not Parquet: {}",
                task.data_file_path
            )));
        }
        let local = local_data_file(root, &task.data_file_path)?;
        let actual = std::fs::metadata(&local)
            .map_err(|e| {
                Error(format!(
                    "cannot stat active file {}: {e}",
                    task.data_file_path
                ))
            })?
            .len();
        if actual != task.length {
            return Err(Error(format!(
                "active file size differs from manifest: {} (expected {}, found {})",
                task.data_file_path, task.length, actual
            )));
        }
        let file_mass = parser.read_masses(&local).map_err(|e| {
            Error(format!(
                "cannot read active file {}: {e}",
                task.data_file_path
            ))
        })?;
        file_bytes = checked_sum(file_bytes, actual)?;
        mass.add(file_mass).map_err(parquet_error)?;
    }

    Ok(MeasuredFiles {
        file_bytes,
        mass: mass.finish(),
        delete_files,
        position_delete_files,
        equality_delete_files,
    })
}

fn collect_delete_files(
    task: &FileScanTask,
    all: &mut BTreeSet<String>,
    position: &mut BTreeSet<String>,
    equality: &mut BTreeSet<String>,
) {
    for delete in &task.deletes {
        all.insert(delete.file_path.clone());
        match delete.file_type {
            DataContentType::PositionDeletes => {
                position.insert(delete.file_path.clone());
            }
            DataContentType::EqualityDeletes => {
                equality.insert(delete.file_path.clone());
            }
            DataContentType::Data => {}
        }
    }
}

fn build_report(snapshot_id: Option<i64>, measured: MeasuredFiles) -> Result<TableReport, Error> {
    let mass = measured.mass.file_mass();
    let mut tree = bytemass::aggregate(&bytemass::read(&mass));
    tree.label = match snapshot_id {
        Some(snapshot_id) => format!("Iceberg snapshot {snapshot_id} (physical bytes/row)"),
        None => "Iceberg table (no current snapshot)".into(),
    };
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
        snapshot_id,
        file_count,
        physical_rows: num_rows,
        file_bytes: measured.file_bytes,
        compressed_column_bytes,
        uncompressed_column_bytes,
        delete_file_count: measured.delete_files.len(),
        position_delete_file_count: measured.position_delete_files.len(),
        equality_delete_file_count: measured.equality_delete_files.len(),
        columns,
        tree,
    })
}

/// Serialize the snapshot report as pretty-printed JSON.
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
        bytemass::render(&report.tree),
    )
}

fn local_table_root(location: &str, metadata_path: &Path) -> Result<PathBuf, Error> {
    let url = Url::parse(location).map_err(|e| {
        Error(format!(
            "Iceberg table location is not a local file URI: {e}"
        ))
    })?;
    if url.scheme() != "file" || url.host_str().is_some() {
        return Err(Error(format!(
            "Iceberg table location is not local: {location}"
        )));
    }
    let root = url
        .to_file_path()
        .map_err(|()| {
            Error(format!(
                "cannot convert table location to a local path: {location}"
            ))
        })?
        .canonicalize()
        .map_err(|e| Error(format!("cannot open table location {location}: {e}")))?;
    if !metadata_path.starts_with(&root) {
        return Err(Error(format!(
            "metadata file is outside the Iceberg table location: {}",
            metadata_path.display()
        )));
    }
    Ok(root)
}

fn local_data_file(root: &Path, location: &str) -> Result<PathBuf, Error> {
    let url = Url::parse(location).map_err(|e| {
        Error(format!(
            "active data file is not a local file URI: {location}: {e}"
        ))
    })?;
    if url.scheme() != "file" || url.host_str().is_some() {
        return Err(Error(format!("active data file is not local: {location}")));
    }
    let local = url
        .to_file_path()
        .map_err(|()| {
            Error(format!(
                "cannot convert active data file to a local path: {location}"
            ))
        })?
        .canonicalize()
        .map_err(|e| Error(format!("cannot open active data file {location}: {e}")))?;
    if !local.starts_with(root) {
        return Err(Error(format!(
            "active data file is outside the Iceberg table location: {location}"
        )));
    }
    Ok(local)
}

fn iceberg_error(error: iceberg::Error) -> Error {
    Error(format!("cannot resolve snapshot: {error}"))
}

fn parquet_error(error: crate::parquet_helpers::Error) -> Error {
    Error(error.to_string())
}

fn checked_sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("storage totals exceed u64".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_paths_require_file_uris_and_containment() {
        let table = tempfile::tempdir().unwrap();
        let metadata = table.path().join("metadata.json");
        std::fs::write(&metadata, "{}").unwrap();
        let metadata = metadata.canonicalize().unwrap();
        let root = local_table_root(
            &Url::from_directory_path(table.path()).unwrap().to_string(),
            &metadata,
        )
        .unwrap();

        let data = table.path().join("data.parquet");
        std::fs::write(&data, b"data").unwrap();
        assert_eq!(
            local_data_file(&root, &Url::from_file_path(&data).unwrap().to_string()).unwrap(),
            data.canonicalize().unwrap()
        );
        assert!(local_data_file(&root, "s3://bucket/data.parquet").is_err());
    }

    #[test]
    fn report_counts_distinct_delete_files_without_applying_them() {
        let mut delete_files = BTreeSet::new();
        let mut position = BTreeSet::new();
        let mut equality = BTreeSet::new();
        let task = FileScanTask {
            start: 0,
            length: 0,
            record_count: Some(0),
            data_file_path: "file:///table/data.parquet".into(),
            data_file_format: DataFileFormat::Parquet,
            schema: std::sync::Arc::new(iceberg::spec::Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![
                iceberg::scan::FileScanTaskDeleteFile {
                    file_path: "file:///table/delete.pos".into(),
                    file_type: DataContentType::PositionDeletes,
                    partition_spec_id: 0,
                    equality_ids: None,
                },
                iceberg::scan::FileScanTaskDeleteFile {
                    file_path: "file:///table/delete.eq".into(),
                    file_type: DataContentType::EqualityDeletes,
                    partition_spec_id: 0,
                    equality_ids: Some(vec![1]),
                },
            ],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        };
        collect_delete_files(&task, &mut delete_files, &mut position, &mut equality);
        assert_eq!(delete_files.len(), 2);
        assert_eq!(position.len(), 1);
        assert_eq!(equality.len(), 1);
    }
}

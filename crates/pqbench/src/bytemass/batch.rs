//! Nested collections of independent tables, analyzed with bounded concurrency.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use futures::{stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use super::{ColumnMassSummary, FooterReader, MassSummary};
use crate::parquet_helpers::Error;

pub use super::batch_d3::render_html;
pub use super::batch_reports::{save_reports, ReportFormat};

/// Versioned input or output with optional grouping levels.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, bound(deserialize = "T: Deserialize<'de>"))]
pub struct Collection<T> {
    pub kind: String,
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lake: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub catalogs: Vec<Catalog<T>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<Schema<T>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<T>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, bound(deserialize = "T: Deserialize<'de>"))]
pub struct Catalog<T> {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<Schema<T>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<T>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, bound(deserialize = "T: Deserialize<'de>"))]
pub struct Schema<T> {
    pub name: String,
    #[serde(default)]
    pub tables: Vec<T>,
}

/// A named invocation of the existing Parquet or Delta analysis.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    #[serde(default)]
    pub format: Format,
    /// Optional Delta snapshot version (as on `pqbench delta --version`).
    #[serde(default)]
    pub snapshot_version: Option<u64>,
    pub source: Source,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    #[default]
    Parquet,
    Delta,
}

/// The same source document accepted by PR #18. Credentials are input-only.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub kind: String,
    pub version: u32,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// A complete table measurement. Failed tables never contribute partial bytes.
#[derive(Clone, Serialize)]
pub struct Analysis {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    pub file_count: usize,
    pub physical_rows: u64,
    pub file_bytes: u64,
    pub compressed_column_bytes: u64,
    pub uncompressed_column_bytes: u64,
    pub columns: Vec<ColumnMassSummary>,
}

#[derive(Clone, Serialize)]
pub struct TableResult {
    pub name: String,
    #[serde(flatten)]
    pub outcome: Outcome,
}

#[derive(Clone, Serialize)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Outcome {
    Complete { analysis: Analysis },
    Failed { error: String },
}

impl<T> Collection<T> {
    /// Tables in stable traversal order, independent of completion order.
    pub fn tables(&self) -> Vec<&T> {
        self.tables
            .iter()
            .chain(self.schemas.iter().flat_map(|s| &s.tables))
            .chain(self.catalogs.iter().flat_map(|c| {
                c.tables
                    .iter()
                    .chain(c.schemas.iter().flat_map(|s| &s.tables))
            }))
            .collect()
    }

    fn map<R>(self, mut f: impl FnMut(T) -> R) -> Collection<R> {
        let tables = self.tables.into_iter().map(&mut f).collect();
        let schemas = self
            .schemas
            .into_iter()
            .map(|s| Schema {
                name: s.name,
                tables: s.tables.into_iter().map(&mut f).collect(),
            })
            .collect();
        let catalogs = self
            .catalogs
            .into_iter()
            .map(|c| {
                let tables = c.tables.into_iter().map(&mut f).collect();
                let schemas = c
                    .schemas
                    .into_iter()
                    .map(|s| Schema {
                        name: s.name,
                        tables: s.tables.into_iter().map(&mut f).collect(),
                    })
                    .collect();
                Catalog {
                    name: c.name,
                    tables,
                    schemas,
                }
            })
            .collect();
        Collection {
            kind: self.kind,
            version: self.version,
            lake: self.lake,
            catalogs,
            schemas,
            tables,
        }
    }
}

impl Collection<Table> {
    /// Validate the entire document before starting any storage reads.
    pub fn validate(&self) -> Result<(), Error> {
        if self.kind != "pqbench.collection" || self.version != 1 {
            return Err(Error("expected kind `pqbench.collection` version 1".into()));
        }
        if let Some(lake) = &self.lake {
            validate_name(lake)?;
        }
        unique_names(self.catalogs.iter().map(|c| c.name.as_str()))?;
        validate_group(&self.tables, &self.schemas)?;
        for catalog in &self.catalogs {
            validate_group(&catalog.tables, &catalog.schemas)?;
        }
        for table in self.tables() {
            let source = &table.source;
            if source.kind != "pqbench.remote-source" || source.version != 1 {
                return Err(Error(
                    "expected table source kind `pqbench.remote-source` version 1".into(),
                ));
            }
            if source.inputs.is_empty() || source.inputs.iter().any(|s| s.trim().is_empty()) {
                return Err(Error("table source needs nonempty inputs".into()));
            }
            if source.inputs.iter().collect::<BTreeSet<_>>().len() != source.inputs.len() {
                return Err(Error("duplicate input in a table".into()));
            }
            if source.env.keys().any(|key| !key.starts_with("AWS_")) {
                return Err(Error("source env may only contain AWS_* names".into()));
            }
            if matches!(table.format, Format::Delta) && source.inputs.len() != 1 {
                return Err(Error("a Delta source must name exactly one table".into()));
            }
            if matches!(table.format, Format::Parquet) && table.snapshot_version.is_some() {
                return Err(Error(
                    "snapshot_version only applies to Delta tables".into(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_name(name: &str) -> Result<(), Error> {
    if name.trim().is_empty() {
        return Err(Error("names must not be empty".into()));
    }
    Ok(())
}

fn unique_names<'a>(names: impl Iterator<Item = &'a str>) -> Result<(), Error> {
    let mut seen = BTreeSet::new();
    for name in names {
        validate_name(name)?;
        if !seen.insert(name) {
            return Err(Error(format!("duplicate name: {name}")));
        }
    }
    Ok(())
}

fn validate_group(tables: &[Table], schemas: &[Schema<Table>]) -> Result<(), Error> {
    unique_names(tables.iter().map(|t| t.name.as_str()))?;
    unique_names(schemas.iter().map(|s| s.name.as_str()))?;
    for schema in schemas {
        unique_names(schema.tables.iter().map(|t| t.name.as_str()))?;
    }
    Ok(())
}

/// Analyze tables concurrently, retaining the input hierarchy and order.
///
/// Run within a Tokio runtime. `file_jobs` is shared across all active tables;
/// it bounds footer reads, not Delta transaction-log requests. Table errors are
/// returned in place and do not cancel other tables.
///
/// # Errors
/// Invalid collection structure fails before I/O begins.
pub async fn analyze(
    collection: Collection<Table>,
    table_jobs: NonZeroUsize,
    file_jobs: NonZeroUsize,
) -> Result<Collection<TableResult>, Error> {
    collection.validate()?;
    let reader = FooterReader::new(file_jobs)?;
    let tables = collection.tables();
    let mut results = stream::iter(tables.into_iter().enumerate().map(|(index, table)| {
        let reader = &reader;
        async move {
            let result = measure(table, reader).await;
            let outcome = match result {
                Ok(analysis) => Outcome::Complete { analysis },
                Err(error) => Outcome::Failed {
                    error: redact(error.to_string(), &table.source),
                },
            };
            (index, outcome)
        }
    }))
    .buffer_unordered(table_jobs.get())
    .collect::<Vec<_>>()
    .await;
    results.sort_unstable_by_key(|(index, _)| *index);
    let mut results = results.into_iter();
    let mut report = collection.map(|table| TableResult {
        name: table.name,
        outcome: results.next().expect("one result per table").1,
    });
    report.kind = "pqbench.collection-report".into();
    Ok(report)
}

// Backend errors can include configuration values; never echo supplied options.
fn redact(mut error: String, source: &Source) -> String {
    let options = &source.env;
    let mut values: Vec<_> = options.values().filter(|v| !v.is_empty()).collect();
    values.sort_by_key(|v| std::cmp::Reverse(v.len()));
    for value in values {
        error = error.replace(value, "[redacted]");
    }
    error
}

async fn measure(table: &Table, reader: &FooterReader) -> Result<Analysis, Error> {
    let inputs = &table.source.inputs;
    let object_store_options = &table.source.env;
    match table.format {
        Format::Parquet => {
            let mut files = stream::iter(
                inputs
                    .iter()
                    .map(|input| reader.read(input, object_store_options)),
            )
            .buffer_unordered(reader.concurrency());
            let mut accumulator = MassAccumulator::new();
            let mut file_bytes = 0;
            while let Some((size, mass)) = files.try_next().await? {
                file_bytes = sum(file_bytes, size)?;
                accumulator.add(mass)?;
            }
            build_analysis(accumulator.finish(), file_bytes, None)
        }
        Format::Delta => {
            let uri = &inputs[0];
            let version = table.snapshot_version;
            #[cfg(feature = "delta")]
            {
                let report = crate::table::delta::delta_with_options(
                    &crate::table::delta::DeltaRequest {
                        table: uri.clone(),
                        version,
                    },
                    object_store_options,
                    reader,
                )
                .await
                .map_err(|e| Error(e.to_string()))?;
                let mass = MassSummary {
                    file_count: report.file_count,
                    num_rows: report.physical_rows,
                    columns: report
                        .columns
                        .into_iter()
                        .map(|c| ColumnMassSummary {
                            path: c.path,
                            compressed_bytes: c.compressed_bytes,
                            uncompressed_bytes: c.uncompressed_bytes,
                            codecs: c.codecs,
                        })
                        .collect(),
                };
                build_analysis(mass, report.file_bytes, Some(report.version))
            }
            #[cfg(not(feature = "delta"))]
            {
                let _ = (uri, version, object_store_options);
                Err(Error(
                    "Delta collections require the `delta` feature (`delta-s3` for S3)".into(),
                ))
            }
        }
    }
}

fn build_analysis(
    mass: MassSummary,
    file_bytes: u64,
    version: Option<u64>,
) -> Result<Analysis, Error> {
    let mut compressed = 0;
    let mut uncompressed = 0;
    for column in &mass.columns {
        compressed = sum(compressed, column.compressed_bytes)?;
        uncompressed = sum(uncompressed, column.uncompressed_bytes)?;
    }
    Ok(Analysis {
        version,
        file_count: mass.file_count,
        physical_rows: mass.num_rows,
        file_bytes,
        compressed_column_bytes: compressed,
        uncompressed_column_bytes: uncompressed,
        columns: mass.columns,
    })
}

fn sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("storage totals exceed u64".into()))
}

/// Incremental fold: retain column totals, not every file's footer.
#[derive(Default)]
struct MassAccumulator {
    files: usize,
    rows: u64,
    columns: BTreeMap<String, ColumnMassSummary>,
}

impl MassAccumulator {
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, mass: crate::parquet_helpers::FileMass) -> Result<(), Error> {
        self.files = self
            .files
            .checked_add(1)
            .ok_or_else(|| Error("file count exceeds usize".into()))?;
        self.rows = sum(self.rows, mass.num_rows)?;
        for column in mass.columns {
            let total =
                self.columns
                    .entry(column.path.clone())
                    .or_insert_with(|| ColumnMassSummary {
                        path: column.path,
                        compressed_bytes: 0,
                        uncompressed_bytes: 0,
                        codecs: BTreeSet::new(),
                    });
            total.compressed_bytes = sum(total.compressed_bytes, column.bytes)?;
            total.uncompressed_bytes = sum(total.uncompressed_bytes, column.uncompressed_bytes)?;
            total.codecs.insert(column.codec);
        }
        Ok(())
    }

    fn finish(self) -> MassSummary {
        MassSummary {
            file_count: self.files,
            num_rows: self.rows,
            columns: self.columns.into_values().collect(),
        }
    }
}

impl Collection<TableResult> {
    pub fn failed_tables(&self) -> usize {
        self.tables()
            .iter()
            .filter(|t| matches!(t.outcome, Outcome::Failed { .. }))
            .count()
    }
}

/// Serialize the output tree: the input hierarchy with a result on each table.
///
/// # Errors
/// Returns a serialization error.
pub fn render_json(report: &Collection<TableResult>) -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(report)?)
}

#![cfg(feature = "iceberg")]

mod support;

use std::fs;
use std::path::Path;

use apache_avro::{Schema, Writer};
use pqbench::parquet_helpers::{default_metadata_parser, MetadataParser};
use pqbench::table::iceberg::{iceberg, render_json, render_text, IcebergRequest};
use serde::Serialize;
use support::write_parquet;
use url::Url;

fn request(metadata: impl Into<String>, snapshot_id: Option<i64>) -> IcebergRequest {
    IcebergRequest {
        metadata: metadata.into(),
        snapshot_id,
    }
}

#[derive(Serialize)]
struct ManifestListRow {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32,
}

#[derive(Serialize)]
struct ManifestRow {
    status: i32,
    data_file: ManifestDataFile,
}

#[derive(Serialize)]
struct ManifestDataFile {
    content: i32,
    file_path: String,
    file_format: String,
    partition: EmptyPartition,
    record_count: i64,
    file_size_in_bytes: i64,
}

#[derive(Serialize)]
struct EmptyPartition {}

const MANIFEST_LIST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int", "default": 0}
  ]
}"#;

const MANIFEST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "data_file",
      "fields": [
        {"name": "content", "type": "int", "default": 0},
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"}
      ]
    }}
  ]
}"#;

struct Fixture {
    _directory: tempfile::TempDir,
    metadata: std::path::PathBuf,
    first_size: u64,
    second_size: u64,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let data = root.join("data");
        let metadata_dir = root.join("metadata");
        fs::create_dir_all(&data).unwrap();
        fs::create_dir_all(&metadata_dir).unwrap();

        let first = data.join("first.parquet");
        let second = data.join("second.parquet");
        write_parquet(&first, 3);
        write_parquet(&second, 5);
        let first_size = fs::metadata(&first).unwrap().len();
        let second_size = fs::metadata(&second).unwrap().len();

        let v0 = write_manifest(
            &metadata_dir.join("m0.avro"),
            &[data_entry(file_uri(&first), first_size, 3, 0)],
        );
        let v1 = write_manifest(
            &metadata_dir.join("m1.avro"),
            &[
                data_entry(file_uri(&first), first_size, 3, 0),
                data_entry(file_uri(&second), second_size, 5, 0),
            ],
        );
        let deletes = write_manifest(
            &metadata_dir.join("d1.avro"),
            &[delete_entry(file_uri(&data.join("deletes.parquet")))],
        );
        let list0 = write_manifest_list(&metadata_dir.join("snap-0.avro"), &[(&v0, 0)]);
        let list1 = write_manifest_list(
            &metadata_dir.join("snap-1.avro"),
            &[(&v1, 0), (&deletes, 1)],
        );

        let metadata = metadata_dir.join("v1.metadata.json");
        fs::write(
            &metadata,
            serde_json::to_vec_pretty(&serde_json::json!({
                "format-version": 2,
                "table-uuid": "11111111-1111-1111-1111-111111111111",
                "location": dir_uri(root),
                "last-updated-ms": 0,
                "last-column-id": 1,
                "current-schema-id": 0,
                "schemas": [{"type":"struct","schema-id":0,"fields":[
                    {"id":1,"name":"id","required":true,"type":"long"}
                ]}],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id":0,"fields":[]}],
                "last-partition-id": 999,
                "current-snapshot-id": 1,
                "snapshots": [
                    {
                        "snapshot-id": 0,
                        "sequence-number": 0,
                        "timestamp-ms": 0,
                        "manifest-list": file_uri(&list0),
                        "schema-id": 0
                    },
                    {
                        "snapshot-id": 1,
                        "sequence-number": 1,
                        "timestamp-ms": 1,
                        "manifest-list": file_uri(&list1),
                        "schema-id": 0
                    }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        Self {
            _directory: directory,
            metadata,
            first_size,
            second_size,
        }
    }
}

fn data_entry(path: String, size: u64, rows: i64, content: i32) -> ManifestRow {
    ManifestRow {
        status: 1,
        data_file: ManifestDataFile {
            content,
            file_path: path,
            file_format: "PARQUET".into(),
            partition: EmptyPartition {},
            record_count: rows,
            file_size_in_bytes: i64::try_from(size).unwrap(),
        },
    }
}

fn delete_entry(path: String) -> ManifestRow {
    data_entry(path, 1, 1, 1)
}

fn write_manifest(path: &Path, rows: &[ManifestRow]) -> std::path::PathBuf {
    write_avro(path, MANIFEST_SCHEMA, rows)
}

fn write_manifest_list(path: &Path, manifests: &[(&Path, i32)]) -> std::path::PathBuf {
    let rows: Vec<_> = manifests
        .iter()
        .map(|(manifest, content)| ManifestListRow {
            manifest_path: file_uri(manifest),
            manifest_length: i64::try_from(fs::metadata(manifest).unwrap().len()).unwrap(),
            partition_spec_id: 0,
            content: *content,
        })
        .collect();
    write_avro(path, MANIFEST_LIST_SCHEMA, &rows)
}

fn write_avro<T: Serialize>(path: &Path, schema: &str, rows: &[T]) -> std::path::PathBuf {
    let schema = Schema::parse_str(schema).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    for row in rows {
        writer.append_ser(row).unwrap();
    }
    fs::write(path, writer.into_inner().unwrap()).unwrap();
    path.to_path_buf()
}

fn file_uri(path: &Path) -> String {
    Url::from_file_path(path).unwrap().into()
}

fn dir_uri(path: &Path) -> String {
    Url::from_directory_path(path).unwrap().into()
}

#[tokio::test]
async fn resolves_snapshots_and_counts_delete_files_without_applying_them() {
    let fixture = Fixture::new();
    let previous = iceberg(&request(fixture.metadata.to_string_lossy(), Some(0)))
        .await
        .unwrap();
    assert_eq!(previous.snapshot_id, Some(0));
    assert_eq!(previous.file_count, 1);
    assert_eq!(previous.physical_rows, 3);
    assert_eq!(previous.file_bytes, fixture.first_size);
    assert_eq!(previous.delete_file_count, 0);

    let latest = iceberg(&request(fixture.metadata.to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(latest.snapshot_id, Some(1));
    assert_eq!(latest.file_count, 2);
    assert_eq!(latest.physical_rows, 8);
    assert_eq!(latest.file_bytes, fixture.first_size + fixture.second_size);
    assert_eq!(latest.delete_file_count, 1);
    assert_eq!(latest.position_delete_file_count, 1);
    assert_eq!(latest.equality_delete_file_count, 0);

    let mass = default_metadata_parser()
        .read_masses(
            &fixture
                .metadata
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("data/first.parquet"),
        )
        .unwrap();
    assert_eq!(latest.columns[0].path, "id");
    assert!(latest.compressed_column_bytes >= mass.columns[0].bytes);
    assert_eq!(
        latest.compressed_bytes_per_row(),
        latest.compressed_column_bytes as f64 / 8.0
    );
    let report: serde_json::Value = serde_json::from_str(&render_json(&latest).unwrap()).unwrap();
    assert_eq!(report["physical_rows"], 8);
    assert!(render_text(&latest)
        .unwrap()
        .contains("delete files: 1 (position: 1, equality: 0, not applied)"));
}

#[tokio::test]
async fn reads_a_metadata_uri_with_the_public_remote_api() {
    let fixture = Fixture::new();
    let uri = Url::from_file_path(&fixture.metadata).unwrap();
    let report = iceberg(&request(uri.as_str(), None)).await.unwrap();
    assert_eq!(report.snapshot_id, Some(1));
    assert_eq!(report.physical_rows, 8);
}

#[tokio::test]
async fn empty_snapshot_has_zero_totals() {
    let fixture = Fixture::new();
    let metadata = fs::read_to_string(&fixture.metadata).unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&metadata).unwrap();
    value["current-snapshot-id"] = serde_json::json!(-1);
    value["snapshots"] = serde_json::json!([]);
    fs::write(&fixture.metadata, serde_json::to_vec(&value).unwrap()).unwrap();
    let report = iceberg(&request(fixture.metadata.to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(report.snapshot_id, None);
    assert_eq!(report.file_count, 0);
    assert_eq!(report.physical_rows, 0);
    assert_eq!(report.file_bytes, 0);
    assert_eq!(report.compressed_bytes_per_row(), 0.0);
}

#[tokio::test]
async fn missing_or_changed_active_files_fail_without_partial_results() {
    let fixture = Fixture::new();
    let data = fixture
        .metadata
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("data/second.parquet");
    fs::write(&data, b"changed").unwrap();
    let error = iceberg(&request(fixture.metadata.to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("size differs from manifest"), "{error}");
    fs::remove_file(&data).unwrap();
    let error = iceberg(&request(fixture.metadata.to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("cannot open active file"), "{error}");
}

#[tokio::test]
async fn rejects_missing_snapshots_and_external_data_paths() {
    let fixture = Fixture::new();
    assert!(
        iceberg(&request(fixture.metadata.to_string_lossy(), Some(99)))
            .await
            .is_err()
    );

    let outside = tempfile::tempdir().unwrap();
    write_parquet(&outside.path().join("outside.parquet"), 2);
    let metadata_dir = fixture.metadata.parent().unwrap();
    let escaped = write_manifest(
        &metadata_dir.join("escape.avro"),
        &[data_entry(
            file_uri(&outside.path().join("outside.parquet")),
            fs::metadata(outside.path().join("outside.parquet"))
                .unwrap()
                .len(),
            2,
            0,
        )],
    );
    let list = write_manifest_list(&metadata_dir.join("snap-escape.avro"), &[(&escaped, 0)]);
    let mut value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&fixture.metadata).unwrap()).unwrap();
    value["current-snapshot-id"] = serde_json::json!(9);
    value["snapshots"] = serde_json::json!([{
        "snapshot-id": 9,
        "sequence-number": 9,
        "timestamp-ms": 9,
        "manifest-list": file_uri(&list),
        "schema-id": 0
    }]);
    fs::write(&fixture.metadata, serde_json::to_vec(&value).unwrap()).unwrap();
    let error = iceberg(&request(fixture.metadata.to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("outside the Iceberg table location"),
        "{error}"
    );
}

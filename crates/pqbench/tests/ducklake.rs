#![cfg(feature = "ducklake")]

mod support;

use std::fs;

use pqbench::table::ducklake::{ducklake, render_json, render_text, DuckLakeRequest};
use rusqlite::{params, Connection};
use support::write_parquet;

fn request(catalog: impl Into<String>, snapshot: Option<u64>) -> DuckLakeRequest {
    DuckLakeRequest {
        catalog: catalog.into(),
        schema: "main".into(),
        table: "events".into(),
        snapshot,
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    catalog: std::path::PathBuf,
    first_size: u64,
    second_size: u64,
    delete_size: u64,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let catalog = directory.path().join("catalog.sqlite");
        let data = directory.path().join("files");
        let table = data.join("main/events");
        let first = table.join("first.parquet");
        let second = table.join("second.parquet");
        let delete = table.join("deletes.parquet");
        write_parquet(&first, 3);
        write_parquet(&second, 5);
        fs::write(&delete, b"delete metadata").unwrap();
        let first_size = fs::metadata(&first).unwrap().len();
        let second_size = fs::metadata(&second).unwrap().len();
        let delete_size = fs::metadata(&delete).unwrap().len();

        let connection = Connection::open(&catalog).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE ducklake_metadata(key VARCHAR, value VARCHAR, scope VARCHAR, scope_id BIGINT);
                 CREATE TABLE ducklake_snapshot(snapshot_id BIGINT, snapshot_time VARCHAR, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);
                 CREATE TABLE ducklake_schema(schema_id BIGINT, schema_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BIGINT);
                 CREATE TABLE ducklake_table(table_id BIGINT, table_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BIGINT);
                 CREATE TABLE ducklake_data_file(data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BIGINT, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT);
                 CREATE TABLE ducklake_delete_file(delete_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BIGINT, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT);
                 CREATE TABLE ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT);
                 INSERT INTO ducklake_metadata VALUES ('version', '0.3', NULL, NULL);
                 INSERT INTO ducklake_metadata VALUES ('encrypted', 'false', NULL, NULL);
                 INSERT INTO ducklake_snapshot VALUES (0, '2026-01-01', 0, 2, 2), (1, '2026-01-02', 1, 2, 3);
                 INSERT INTO ducklake_schema VALUES (1, 'schema', 0, NULL, 'main', 'main/', 1);
                 INSERT INTO ducklake_table VALUES (1, 'table', 0, NULL, 1, 'events', 'events/', 1);",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO ducklake_metadata VALUES ('data_path', ?, NULL, NULL)",
                params![data.to_string_lossy().as_ref()],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO ducklake_data_file VALUES
                 (1, 1, 0, NULL, 0, 'first.parquet', 1, 'parquet', 3, ?, NULL, 0, NULL, NULL, NULL, NULL),
                 (2, 1, 1, NULL, 1, 'second.parquet', 1, 'parquet', 5, ?, NULL, 3, NULL, NULL, NULL, NULL)",
                params![first_size as i64, second_size as i64],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO ducklake_delete_file VALUES
                 (1, 1, 1, NULL, 1, 'deletes.parquet', 1, 'parquet', 1, ?, NULL, NULL, NULL)",
                params![delete_size as i64],
            )
            .unwrap();
        Self {
            _directory: directory,
            catalog,
            first_size,
            second_size,
            delete_size,
        }
    }
}

#[tokio::test]
async fn resolves_latest_and_explicit_snapshots_and_reports_deletes() {
    let fixture = Fixture::new();
    let previous = ducklake(&request(fixture.catalog.to_string_lossy(), Some(0)))
        .await
        .unwrap();
    assert_eq!(previous.snapshot, 0);
    assert_eq!(previous.file_count, 1);
    assert_eq!(previous.physical_rows, 3);
    assert_eq!(previous.file_bytes, fixture.first_size);
    assert_eq!(previous.delete_file_count, 0);

    let latest = ducklake(&request(fixture.catalog.to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(latest.snapshot, 1);
    assert_eq!(latest.schema, "main");
    assert_eq!(latest.table, "events");
    assert_eq!(latest.file_count, 2);
    assert_eq!(latest.physical_rows, 8);
    assert_eq!(latest.file_bytes, fixture.first_size + fixture.second_size);
    assert_eq!(latest.delete_file_count, 1);
    assert_eq!(latest.delete_file_bytes, fixture.delete_size);
    assert_eq!(latest.deleted_rows, 1);
    assert_eq!(latest.delete_files[0].path, "deletes.parquet");
    assert!(render_text(&latest)
        .unwrap()
        .contains("active delete files: 1"));
    let json: serde_json::Value = serde_json::from_str(&render_json(&latest).unwrap()).unwrap();
    assert_eq!(json["snapshot"], 1);
    assert_eq!(json["delete_files"][0]["deleted_rows"], 1);
}

#[tokio::test]
async fn rejects_changed_files_and_unsupported_forms() {
    let fixture = Fixture::new();
    let connection = Connection::open(&fixture.catalog).unwrap();
    connection
        .execute(
            "UPDATE ducklake_data_file SET file_size_bytes = 1 WHERE data_file_id = 1",
            [],
        )
        .unwrap();
    let error = ducklake(&request(fixture.catalog.to_string_lossy(), Some(0)))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("size differs from metadata"), "{error}");

    connection
        .execute(
            "UPDATE ducklake_data_file SET file_size_bytes = ?, file_format = 'csv' WHERE data_file_id = 1",
            params![fixture.first_size as i64],
        )
        .unwrap();
    let error = ducklake(&request(fixture.catalog.to_string_lossy(), Some(0)))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("unsupported DuckLake data file format"),
        "{error}"
    );

    connection
        .execute(
            "UPDATE ducklake_data_file SET file_format = 'parquet' WHERE data_file_id = 1",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO ducklake_inlined_data_tables VALUES (1, 'events_inlined', 1)",
            [],
        )
        .unwrap();
    let error = ducklake(&request(fixture.catalog.to_string_lossy(), Some(0)))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("inlined data"), "{error}");
}

#[tokio::test]
async fn rejects_missing_snapshots_and_path_escapes() {
    let fixture = Fixture::new();
    assert!(
        ducklake(&request(fixture.catalog.to_string_lossy(), Some(99)))
            .await
            .is_err()
    );

    let outside = tempfile::tempdir().unwrap();
    write_parquet(&outside.path().join("outside.parquet"), 2);
    let connection = Connection::open(&fixture.catalog).unwrap();
    connection
        .execute(
            "UPDATE ducklake_data_file SET path = ?, path_is_relative = 0 WHERE data_file_id = 1",
            params![outside
                .path()
                .join("outside.parquet")
                .to_string_lossy()
                .as_ref()],
        )
        .unwrap();
    let error = ducklake(&request(fixture.catalog.to_string_lossy(), Some(0)))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("outside the DuckLake data_path"), "{error}");
}

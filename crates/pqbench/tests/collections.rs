use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use pqbench::bytemass::{self, batch};
use serde_json::{json, Value};

fn jobs(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn fixture() -> String {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_snappy.parquet"
    )
    .into()
}

fn table(name: &str, inputs: Value) -> Value {
    json!({"name": name, "source": {"kind": "pqbench.remote-source", "version": 1, "inputs": inputs}})
}

fn input(tables: Vec<Value>) -> batch::Collection<batch::Table> {
    serde_json::from_value(json!({"kind": "pqbench.collection", "version": 1, "tables": tables}))
        .unwrap()
}

#[tokio::test]
async fn preserves_hierarchy_and_matches_existing_analysis_with_parallel_reads() {
    let document = json!({"kind":"pqbench.collection", "version":1, "lake":"lake",
    "catalogs":[{"name":"catalog", "schemas":[{"name":"schema", "tables":[
        table("first", json!([fixture()])), table("second", json!([fixture()]))
    ]}]}]});
    let sequential = batch::analyze(
        serde_json::from_value(document.clone()).unwrap(),
        jobs(1),
        jobs(1),
    )
    .await
    .unwrap();
    let parallel = batch::analyze(serde_json::from_value(document).unwrap(), jobs(4), jobs(8))
        .await
        .unwrap();
    let result = serde_json::to_value(&parallel).unwrap();
    assert_eq!(result, serde_json::to_value(sequential).unwrap());
    let rows = bytemass::bytemass(&bytemass::BytemassRequest {
        inputs: vec![fixture()],
    })
    .await
    .unwrap();
    let expected = bytemass::aggregate(&rows).unwrap();
    let tables = &result["catalogs"][0]["schemas"][0]["tables"];
    assert_eq!(tables[0]["name"], "first");
    assert_eq!(tables[1]["name"], "second");
    assert_eq!(tables[0]["analysis"]["physical_rows"], expected.num_rows);
    assert_eq!(
        tables[0]["analysis"]["columns"],
        serde_json::to_value(expected.columns).unwrap()
    );
    assert_eq!(parallel.failed_tables(), 0);
}

#[tokio::test]
async fn failures_stay_in_place_and_sources_are_not_exported() {
    let mut bad = table("bad", json!(["unknown://bucket/super-secret/file"]));
    bad["source"]["env"] = json!({"AWS_SECRET_ACCESS_KEY": "super-secret"});
    let report = batch::analyze(
        input(vec![bad, table("good", json!([fixture()]))]),
        jobs(2),
        jobs(2),
    )
    .await
    .unwrap();
    let value = serde_json::to_value(&report).unwrap();
    assert_eq!(report.failed_tables(), 1);
    assert_eq!(value["tables"][0]["status"], "FAILED");
    assert_eq!(value["tables"][1]["status"], "COMPLETE");
    assert!(value["tables"][0].get("analysis").is_none());
    let json = value.to_string();
    assert!(!json.contains("super-secret"));
    assert!(!json.contains("AWS_SECRET_ACCESS_KEY"));
    assert!(!json.contains("source"));
}

#[tokio::test]
async fn rejects_invalid_documents_before_reading_and_accepts_empty_groups() {
    let duplicate = input(vec![
        table("same", json!([fixture()])),
        table("same", json!([fixture()])),
    ]);
    assert!(batch::analyze(duplicate, jobs(2), jobs(2)).await.is_err());
    let mut unsupported = table("x", json!([fixture()]));
    unsupported["source"]["version"] = json!(2);
    assert!(input(vec![unsupported]).validate().is_err());
    let mut env = table("x", json!([fixture()]));
    env["source"]["env"] = json!({"HOME":"ignored"});
    assert!(input(vec![env]).validate().is_err());
    assert!(input(vec![table("x", json!([]))]).validate().is_err());
    assert!(input(vec![table("x", json!([fixture(), fixture()]))])
        .validate()
        .is_err());
    let empty = batch::analyze(input(vec![]), jobs(2), jobs(2))
        .await
        .unwrap();
    assert!(empty.tables().is_empty());
    assert!(batch::render_html(&empty)
        .unwrap()
        .contains("0 of 0 tables analyzed"));
    assert!(batch::analyze(input(vec![]), jobs(1), jobs(usize::MAX))
        .await
        .is_err());
}

#[tokio::test]
async fn saves_each_group_and_table_without_path_escape_or_html_injection() {
    let document = json!({"kind":"pqbench.collection", "version":1, "lake":"production",
    "catalogs":[{"name":"analytics", "schemas":[{"name":"sales", "tables":[
        table("orders", json!([fixture()])), table("../</script>", json!([fixture()]))
    ]}]}]});
    let report = batch::analyze(serde_json::from_value(document).unwrap(), jobs(2), jobs(2))
        .await
        .unwrap();
    let json: Value = serde_json::from_str(&batch::render_json(&report).unwrap()).unwrap();
    assert_eq!(json["kind"], "pqbench.collection-report");
    assert_eq!(json["lake"], "production");
    assert_eq!(json["catalogs"][0]["name"], "analytics");
    assert_eq!(json["catalogs"][0]["schemas"][0]["name"], "sales");
    assert_eq!(
        json["catalogs"][0]["schemas"][0]["tables"][0]["name"],
        "orders"
    );
    assert_eq!(json, serde_json::to_value(&report).unwrap());
    let html = batch::render_html(&report).unwrap();
    assert!(html.contains("pqbench.collection-report"));
    assert!(html.contains("\"catalogs\""));
    assert!(html.contains("analytics"));
    assert!(html.contains("const tree ="));
    assert!(html.contains("class=\"split\""));
    assert!(html.contains("d3-hierarchy@3"));
    assert!(html.contains("d3-scale-chromatic@3"));
    assert!(html.contains("group-label"));
    assert!(!html.contains("../</script>"));
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("reports");
    batch::save_reports(&report, &target, batch::ReportFormat::Html).unwrap();
    assert_eq!(
        std::fs::read_to_string(target.join("index.html")).unwrap(),
        html
    );
    assert!(!target.join("catalog-analytics").exists());
    assert!(batch::save_reports(&report, &target, batch::ReportFormat::Html).is_err());
    let json_target = directory.path().join("json");
    batch::save_reports(&report, &json_target, batch::ReportFormat::Json).unwrap();
    let saved: Value =
        serde_json::from_slice(&std::fs::read(json_target.join("index.json")).unwrap()).unwrap();
    assert_eq!(saved, json);
}

/// Block the first I/O worker until a second starts. A sequential implementation
/// times out and fails; a concurrent one releases both immediately. No network
/// or throughput timing is involved, and the timeout only bounds a regression.
fn assert_overlapping_reads(tables: Vec<Value>, table_jobs: usize, file_jobs: usize) {
    let gate = Arc::new((Mutex::new((0, false)), Condvar::new()));
    let worker_gate = gate.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(2)
        .on_thread_start(move || {
            let (lock, condvar) = &*worker_gate;
            let mut state = lock.lock().unwrap();
            state.0 += 1;
            condvar.notify_all();
            let (mut state, timeout) = condvar
                .wait_timeout_while(state, Duration::from_secs(1), |s| s.0 < 2)
                .unwrap();
            if timeout.timed_out() {
                state.1 = true;
            }
        })
        .build()
        .unwrap();
    let report = runtime
        .block_on(batch::analyze(
            input(tables),
            jobs(table_jobs),
            jobs(file_jobs),
        ))
        .unwrap();
    assert_eq!(report.failed_tables(), 0);
    let state = gate.0.lock().unwrap();
    assert_eq!(state.0, 2);
    assert!(!state.1, "footer reads did not overlap");
}

#[test]
fn tables_and_files_both_run_concurrently() {
    assert_overlapping_reads(
        vec![
            table("a", json!([fixture()])),
            table("b", json!([fixture()])),
        ],
        2,
        2,
    );
    let directory = tempfile::tempdir().unwrap();
    let second = directory.path().join("second.parquet");
    std::fs::copy(fixture(), &second).unwrap();
    assert_overlapping_reads(vec![table("a", json!([fixture(), second]))], 1, 2);
}

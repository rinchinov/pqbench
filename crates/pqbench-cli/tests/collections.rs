use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Stdio};

fn run(document: &Value, args: &[&str]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .args(["bytemass", "--collection", "-"])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(document.to_string().as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn collection_pipe_emits_results_even_when_one_table_fails() {
    let file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    );
    let document = json!({"kind":"pqbench.collection", "version":1, "tables":[
        {"name":"good", "source":{"kind":"pqbench.remote-source", "version":1, "inputs":[file]}},
        {"name":"bad", "source":{"kind":"pqbench.remote-source", "version":1, "inputs":["/nonexistent/table.parquet"]}}
    ]});
    let output = run(
        &document,
        &["--json", "--table-jobs", "2", "--file-jobs", "2"],
    );
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["kind"], "pqbench.collection-report");
    assert_eq!(report["tables"][0]["status"], "COMPLETE");
    assert_eq!(report["tables"][1]["status"], "FAILED");
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("1 table(s) failed"));
}

#[test]
fn output_dir_writes_hierarchy_and_keeps_stdout_empty() {
    let file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    );
    let document = json!({"kind":"pqbench.collection", "version":1, "lake":"production",
    "catalogs":[{"name":"analytics", "schemas":[{"name":"sales", "tables":[
        {"name":"good", "source":{"kind":"pqbench.remote-source", "version":1, "inputs":[file]}},
        {"name":"bad", "source":{"kind":"pqbench.remote-source", "version":1,
            "inputs":["/nonexistent/table.parquet"]}}
    ]}]}]});
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("reports");
    let output = run(
        &document,
        &[
            "--d3",
            "--output-dir",
            target.to_str().unwrap(),
            "--table-jobs",
            "2",
            "--file-jobs",
            "2",
        ],
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Saved reports"));
    assert!(stderr.contains("1 table(s) failed"));
    let html = std::fs::read_to_string(target.join("index.html")).unwrap();
    assert!(html.contains("pqbench.collection-report"));
    assert!(html.contains("production"));
    assert!(html.contains("analytics"));
    assert!(html.contains("good"));
    assert!(!target.join("catalog-analytics").exists());
}

#[test]
fn rejects_zero_concurrency_and_conflicting_inputs() {
    for args in [
        vec!["--table-jobs", "0"],
        vec!["--file-jobs", "0"],
        vec!["--source", "-"],
        vec!["other.parquet"],
        vec!["--json", "--d3"],
        vec!["--output-dir", "reports"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_pqbench"))
            .args(["bytemass", "--collection", "-"])
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
    }
    let existing = tempfile::tempdir().unwrap();
    let output = run(
        &json!({"kind":"pqbench.collection", "version":1, "tables":[]}),
        &["--json", "--output-dir", existing.path().to_str().unwrap()],
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("new directory"));
}

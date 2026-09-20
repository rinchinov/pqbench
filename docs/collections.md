# Analyze a collection of tables

Feed pqbench a nested list of table invocations. Each table carries the same
`pqbench.remote-source` v1 document as `bytemass --source -` or `delta --source -`:

```json
{
  "kind": "pqbench.collection",
  "version": 1,
  "lake": "production",
  "catalogs": [{
    "name": "analytics",
    "schemas": [{
      "name": "sales",
      "tables": [{
        "name": "orders",
        "format": "delta",
        "source": {
          "kind": "pqbench.remote-source",
          "version": 1,
          "inputs": ["s3://warehouse/sales/orders"],
          "env": {"AWS_REGION": "us-east-1"}
        }
      }]
    }]
  }]
}
```

Lake, catalog and schema levels are optional. The root can contain `catalogs`,
`schemas`, or `tables`; a catalog can contain `schemas` or `tables`. Arrays keep
their input order in the results. Names must be unique among siblings of the
same kind. Empty groups are retained.

`format` defaults to `parquet`: its source inputs name exact Parquet files,
which form one table. `format: "delta"` requires exactly one table root; pqbench
resolves its active snapshot, excluding tombstoned and untracked files. An
optional table-level `snapshot_version` selects a specific Delta version.
Local paths and `file://` URIs work; paths are relative to the working directory.
Collection inputs do not expand globs. Delta needs the `delta` build feature;
S3 Delta needs `delta-s3`. Existing unsupported Delta features remain unsupported.

## Run and save

```bash
cargo build -p pqbench-cli --features delta-s3

target/debug/pqbench bytemass --collection tables.json \
  --table-jobs 4 --file-jobs 32 --json > report.json

cat tables.json | target/debug/pqbench bytemass --collection - \
  --table-jobs 4 --file-jobs 32 --d3 --output-dir reports
```

Four tables can be active simultaneously, sharing a maximum of 32 footer
reads. These are the defaults; use both limits at 1 for sequential execution.
The file limit covers Parquet reads, not Delta transaction-log requests.
Local footer reads run on a blocking pool so they can overlap as well.

`--json` writes the **output tree**: the same lake / catalog / schema / table
nesting as the input, with `status` and `analysis` on each table instead of
`source`. `--d3` embeds that tree in one HTML page: a layer list on the left and a
clickable d3 treemap on the right. A recorded walkthrough and screenshots live
in the [visual demos](demo.md). The map shows two layers at once: the
current group and the next (Bostock nested padding). Groups use Tableau 10;
column leaves use ColorBrewer YlGnBu. Clicking a cell or a list row drills
lake → catalog → schema → table → columns. The treemap loads d3 from a CDN.

`--output-dir` must name a new directory whose parent exists. It writes one
file: `index.json` for the tree, or `index.html` for the page. Existing
directories are not overwritten. Without `--output-dir`, the tree or page goes
to stdout.

Compressed column bytes are additive across tables. Bytes per physical row are
shown only within a table. Active file bytes also include Parquet overhead and
are reported separately. These are active-data measurements, not total bucket
usage.

A failed table gets `status: "FAILED"` and an error in its original position;
successful tables get `status: "COMPLETE"` and an `analysis`. Successful results
are saved even if another table fails; the process then exits nonzero. HTML
shows analysis coverage, and failed or empty tables remain in the layer list
with zero bytes. Snapshots are resolved independently
per table, not as an atomic lake-wide snapshot.

## Databricks CLI → jq → pqbench

The Databricks CLI renders list commands as **JSON arrays**, not REST response
objects such as `{ "tables": [...] }`. Its iterator handles pagination. The
relevant fields are `name`, `data_source_format`, and `storage_location`.
Verified against the official [tables command source](https://github.com/databricks/cli/blob/main/cmd/workspace/tables/tables.go),
[schemas command source](https://github.com/databricks/cli/blob/main/cmd/workspace/schemas/schemas.go),
[JSON iterator renderer](https://github.com/databricks/cli/blob/main/libs/cmdio/render.go),
and [TableInfo model](https://github.com/databricks/databricks-sdk-go/blob/main/service/catalog/model.go).

For one schema, using ambient S3 credentials:

```bash
set -euo pipefail
catalog=analytics
schema=sales

databricks tables list "$catalog" "$schema" --omit-columns --omit-properties -o json |
  jq --arg catalog "$catalog" --arg schema "$schema" '
    {kind: "pqbench.collection", version: 1, catalogs: [{name: $catalog,
      schemas: [{name: $schema, tables: [.[] |
        select(.data_source_format == "DELTA") |
        {name, format: "delta", source: {
          kind: "pqbench.remote-source", version: 1,
          inputs: [.storage_location]}}]}]}]}' |
  target/debug/pqbench bytemass --collection - --d3 --output-dir reports
```

For all schemas in selected catalogs, the supplied Bash helper builds the same
document. With no catalog arguments it discovers all catalogs visible to the
current Databricks identity:

```bash
set -euo pipefail
bash scripts/databricks-collection.sh analytics finance |
  target/debug/pqbench bytemass --collection - \
    --table-jobs 4 --file-jobs 32 --d3 --output-dir reports
```

The helper selects Delta tables and retains empty schemas. Discovery failures
abort before it emits a document. Access to catalog metadata does not grant
access to S3: configure storage credentials separately, or supply each table's
vended credentials in its `source.env`, as in the
[PR #18 credential-vending example](../docker/e2e-lakehouse/README.md#the-same-pipe-against-databricks).

For example, wrap an existing single-table producer without changing its source:

```bash
your-source-producer |
  jq '{kind: "pqbench.collection", version: 1, tables: [
    {name: "orders", format: "delta", source: .}]}' |
  target/debug/pqbench bytemass --collection - --json
```

`source.env` remains AWS-only. In collection mode these values are passed to
each table's storage clients, **without changing the process environment**;
different tables can use different credentials concurrently. Supported keys
are the storage backends' AWS configuration options (access key, secret,
session token, region, endpoint, etc.). Reports omit source documents and
credentials. Pipe credential-bearing inputs rather than saving them to disk.

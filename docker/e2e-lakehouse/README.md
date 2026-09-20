# Catalogs → pqbench

Three catalogs share one S3-compatible object store. Each names a table;
pqbench resolves that table's live Parquet files and measures their footers
through `--source -`. No Spark, no Postgres, no UI: [Unity Catalog
OSS](https://docs.unitycatalog.io/docker_compose/), an [Iceberg REST
fixture](https://iceberg.apache.org/spark-quickstart/), DuckLake metadata in
SQLite, and [rustfs](https://docs.rustfs.com/en/installation/container/docker/).
Unity's official image is a large download (about 2.4 GB).

| Catalog | Table | Objects |
| --- | --- | --- |
| Unity Catalog OSS | `pqbench.demo.events` (external Delta) | `s3://lakehouse/unity/events` |
| Iceberg REST | `demo.events` | `s3://lakehouse/iceberg/` |
| DuckLake | `main.events` | `s3://lakehouse/ducklake/` |

Each table has three rows and two columns (`id`, `label`). The bytes are
committed under [`table/`](table), [`iceberg/`](iceberg), and
[`ducklake/`](ducklake), so a rerun next month measures the same files. The
Iceberg and DuckLake fixtures were written once with pyiceberg 0.10.0 and
DuckDB 1.4.4 against rustfs; those writers are not part of the stand.

## Run it

Needs Docker Compose v2, a Rust toolchain for this branch, Bash, `curl`, and
`jq`. From the repository root:

```bash
make lakehouse
```

That reaches a stand you can query, in steps you can also run alone:

| Target | What it does |
| --- | --- |
| `make lakehouse-up` | builds pqbench with `--features delta-s3,iceberg-s3,ducklake-s3`, starts rustfs, mints the session credential Unity will vend, waits for Unity and Iceberg REST to answer |
| `make lakehouse-seed-s3` | uploads [`table/`](table) to `s3://lakehouse/unity/events` |
| `make lakehouse-seed-unity` | registers `pqbench.demo.events` as an external Delta table |
| `make lakehouse-seed-iceberg` | uploads [`iceberg/`](iceberg) and registers `demo.events` over REST |
| `make lakehouse-seed-ducklake` | uploads [`ducklake/objects/`](ducklake/objects) (SQLite metadata stays local) |
| `make lakehouse` | all of the above, then the checks below |

Every step is idempotent, and the minted credential is cached in
`local/lakehouse/vended.env` for its 12-hour life, so a rerun neither mints a new
session nor restarts Unity. Services stay up afterwards. If a host port is taken,
override it: `UNITY_CATALOG_PORT=18080 make lakehouse`.

## Curl checks

```bash
UC=http://localhost:8080/api/2.1/unity-catalog
S3=http://localhost:9000

# What the catalog holds:
curl -s $UC/tables/pqbench.demo.events |
  jq '{table_id, data_source_format, storage_location}'
```

```json
{
  "table_id": "2fe1f2b8-465c-41a4-9850-729d1af3b6e9",
  "data_source_format": "DELTA",
  "storage_location": "s3://lakehouse/unity/events"
}
```

Now the point of the stand: Unity vends read credentials for that table, and the
bytes are measured with those and nothing else — no key in your shell, no
long-lived key in the pipe.

```bash
TABLE=$(curl -s $UC/tables/pqbench.demo.events)

curl -s -X POST $UC/temporary-table-credentials -H 'Content-Type: application/json' \
    -d "$(jq -c '{table_id, operation: "READ"}' <<< "$TABLE")" |
  jq -c --arg table "$(jq -r .storage_location <<< "$TABLE")" --arg s3 "$S3" \
    '{kind: "pqbench.remote-source", version: 1, inputs: [$table],
      env: (.aws_temp_credentials | {AWS_ACCESS_KEY_ID: .access_key_id,
        AWS_SECRET_ACCESS_KEY: .secret_access_key,
        AWS_SESSION_TOKEN: .session_token, AWS_REGION: "us-east-1",
        AWS_ENDPOINT: $s3, AWS_ENDPOINT_URL: $s3, AWS_ALLOW_HTTP: "true",
        AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"})}' |
  target/debug/pqbench delta --source -
```

```text
delta version: 0
active files: 1
physical rows: 3
active parquet bytes: 796
compressed column bytes: 138
uncompressed column bytes: 133
bytemass: part-00000-5eef9a52-f717-4d78-8e62-d7a2a05c707b-c000.snappy.parquet
column                              bytes/row
label                                   24.00
id                                      22.00
--------------------------------------------
total                                   46.00
```

`make lakehouse` runs exactly this pipe as its last step. Swap the final command
for `--d3 > events-treemap.html` to get a treemap, or drop it and pipe to `jq .`
to read the document itself.

The document is `{"kind": "pqbench.remote-source", "version": 1, "inputs": [...],
"env": {...}}`. The producer answers *what to measure*, so pqbench keeps no
catalog dependency. Storage configuration normally comes from the `AWS_*`
environment; a producer whose catalog vends expiring credentials puts them in
`env` instead, which pqbench applies to its own environment before reading. Only
`AWS_*` names are accepted there, and anything else is a loud error. Both
endpoint names appear because pqbench's object store reads `AWS_ENDPOINT` while
delta-rs reads `AWS_ENDPOINT_URL`; against real AWS neither is needed. `inputs`
is one table for `pqbench delta`, one metadata JSON location for
`pqbench iceberg`, or one SQLite catalog for `pqbench ducklake`.
`pqbench bytemass --source -` takes the same document naming Parquet objects
directly.

One caveat before copying this shape onto real infrastructure. Unity Catalog OSS
mints vended credentials by calling AWS STS `AssumeRole` and cannot send that
call to an S3-compatible endpoint
([unitycatalog#43](https://github.com/unitycatalog/unitycatalog/issues/43)), so
against rustfs it would talk to real AWS and fail. The stand therefore mints a
12-hour session credential from rustfs's own STS and configures Unity with it
(`s3.accessKey.0`/`s3.secretKey.0`/`s3.sessionToken.0`), Unity's preset
credential mode: it vends that credential verbatim. The credential is genuinely
temporary and issued by the object store, but it is not scoped per table, and the
request's `operation` does not narrow it. Against real AWS, Unity would assume a
role and return a policy-scoped session.

## Iceberg REST

The fixture does **not** vend credentials. `loadTable` returns the metadata
JSON location; `pqbench iceberg` reads that snapshot and measures the active
data files. Storage keys stay in `env` as the stand's dummy values.

```bash
ICEBERG=http://localhost:8181
S3=http://localhost:9000

curl -s $ICEBERG/v1/namespaces/demo/tables/events |
  jq '{snapshot: .metadata["current-snapshot-id"], location: .metadata.location}'
```

```json
{
  "snapshot": 3268038499157964613,
  "location": "s3://lakehouse/iceberg/demo/events"
}
```

```bash
S3=http://localhost:9000
curl -s $ICEBERG/v1/namespaces/demo/tables/events |
  jq -c --arg s3 "$S3" '{kind: "pqbench.remote-source", version: 1,
    inputs: [."metadata-location"],
    env: {AWS_ACCESS_KEY_ID: "test", AWS_SECRET_ACCESS_KEY: "test",
      AWS_REGION: "us-east-1", AWS_ENDPOINT: $s3, AWS_ALLOW_HTTP: "true",
      AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"}}' |
  target/debug/pqbench iceberg --source -
```

```text
iceberg snapshot: 3268038499157964613
active data files: 1
physical rows: 3
active parquet bytes: 962
compressed column bytes: 231
uncompressed column bytes: 227
delete files: 0 (position: 0, equality: 0, not applied)
bytemass: 00000-0-672adf8a-08b9-40a5-bd0d-2f2dcbfb965c.parquet
column                              bytes/row
id                                      41.67
label                                   35.33
--------------------------------------------
total                                   77.00
```

To grow the table, add Parquet files and a new snapshot with a real Iceberg
writer, commit the updated `metadata/`, and re-run `make lakehouse-seed-iceberg`.

## DuckLake

DuckLake has no server and does not vend credentials. The catalog is a committed
SQLite file ([`ducklake/metadata.sqlite`](ducklake/metadata.sqlite));
`pqbench ducklake` reads active files from that catalog. Delete files are
counted and not applied. `DATA_INLINING_ROW_LIMIT` was 0 when the fixture was
written, so the three rows are Parquet on S3, not inlined in SQLite.

```bash
S3=http://localhost:9000

jq -n --arg catalog docker/e2e-lakehouse/ducklake/metadata.sqlite --arg s3 "$S3" \
  '{kind: "pqbench.remote-source", version: 1, inputs: [$catalog],
    env: {AWS_ACCESS_KEY_ID: "test", AWS_SECRET_ACCESS_KEY: "test",
      AWS_REGION: "us-east-1", AWS_ENDPOINT: $s3, AWS_ALLOW_HTTP: "true",
      AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"}}' |
  target/debug/pqbench ducklake --source - --table events
```

```text
DuckLake table: main.events
snapshot: 1
active data files: 1
physical rows: 3
active parquet bytes: 354
compressed column bytes: 96
uncompressed column bytes: 92
active delete files: 0
delete-file bytes: 0
deleted rows: 0
bytemass: ducklake-01a0c08f-88d1-771c-9eb8-a5eee4b98992.parquet
column                              bytes/row
label                                   17.33
id                                      14.67
--------------------------------------------
total                                   32.00
```

To grow it, rewrite the fixture with DuckDB (`DATA_INLINING_ROW_LIMIT 0`), commit
the new Parquet plus `metadata.sqlite`, and re-run `make lakehouse-seed-ducklake`.

## The same pipe against Databricks

Databricks Unity Catalog vends genuine STS sessions and its CLI has a command for
that, so the shape survives the move off the stand. The vending response's `url`
is the table's storage path, which is all `pqbench delta` needs — it reads the
log itself, so nothing has to enumerate files:

```bash
TABLE=main.demo.events

databricks temporary-table-credentials generate-temporary-table-credentials \
    --table-id "$(databricks tables get "$TABLE" -o json | jq -r .table_id)" \
    --operation READ -o json |
  jq -c '{kind: "pqbench.remote-source", version: 1, inputs: [.url],
    env: (.aws_temp_credentials | {AWS_ACCESS_KEY_ID: .access_key_id,
      AWS_SECRET_ACCESS_KEY: .secret_access_key,
      AWS_SESSION_TOKEN: .session_token, AWS_REGION: "us-east-1"})}' |
  pqbench delta --source -
```

`--table-id` wants the table's UUID, hence the inner `tables get`. `AWS_REGION`
is the bucket's region. Vending is off until a metastore admin sets
`external_access_enabled` and a catalog owner grants `EXTERNAL USE SCHEMA` on the
schema; `databricks tables get "$TABLE" --include-manifest-capabilities -o json`
says whether a table is eligible at all
([credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)).
The session expires — `expiration_time` is epoch milliseconds — so vend inside
the pipe rather than caching it in your shell. Azure and GCP return
`azure_user_delegation_sas` or `gcp_oauth_token` instead, which an `AWS_*`-only
`env` cannot carry.

## Endpoints and state

Host endpoints bind to loopback: S3 `http://localhost:9000`, Unity Catalog
`http://localhost:8080`, Iceberg REST `http://localhost:8181`. Override with
`RUSTFS_PORT`, `UNITY_CATALOG_PORT`, and `ICEBERG_REST_PORT`.
Containers on the Compose network reach the same objects at `http://rustfs:9000`,
so a pqbench running there wants that endpoint instead of `localhost`.
Credentials are the local dummy values `test` / `test`, region `us-east-1`, HTTP
and path-style S3 — hence `AWS_ALLOW_HTTP` and
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false`.

The images are pinned by tag and digest (`rustfs/rustfs:1.0.0`,
`unitycatalog/unitycatalog:v0.6.0`, `apache/iceberg-rest-fixture:1.10.0`,
`amazon/aws-cli:2.36.49`, `keinos/sqlite3:3.47.2`); bump them deliberately.

Any S3 client reaches the objects — the AWS CLI needs only the endpoint:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:9000 s3 ls s3://lakehouse/ --recursive
compose() { docker compose -f docker/e2e-lakehouse/compose.yaml "$@"; }
compose down     # stop; retain named volumes
compose down -v  # explicit reset of this stand's data only
```

The rustfs objects and Unity's H2 metadata use named volumes.

## Scope

Unity resolves an external Delta table's location and vends a credential for it;
pqbench reads the log and measures the active files' footers. Iceberg REST names
the current metadata JSON; `pqbench iceberg` reads that snapshot. DuckLake has
no server: `pqbench ducklake` reads the committed SQLite catalog. Vending on
Unity is preset-credential mode (see [above](#curl-checks)), so it does **not**
test per-table scoping, IAM enforcement, or Databricks managed tables.
Measurement is physical Parquet storage, not logical rows after deletions.
rustfs speaks the S3 API but is not AWS. This stand is opt-in and separate from
the fast Rust test suite.

References: [Unity Catalog Compose](https://docs.unitycatalog.io/docker_compose/),
[Unity Catalog credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending),
[Iceberg REST](https://iceberg.apache.org/spec/#rest-catalog-api),
[DuckLake file listing](https://ducklake.select/docs/stable/duckdb/metadata/list_files),
[rustfs in Docker](https://docs.rustfs.com/en/installation/container/docker/),
[delta-rs](https://delta-io.github.io/delta-rs/).

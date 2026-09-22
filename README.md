# pqbench

*lzbench for parquet.*

Measure how well each codec compresses a parquet file and how many on-disk bytes
each column costs.

## Quick start

Docker is the fastest way to try it — no Rust toolchain, no build:

```sh
docker pull pqbench/pqbench:latest
docker run --rm -v "$PWD:/data:ro" pqbench/pqbench:latest bytemass /data/your.parquet
```

The published image is a portable baseline build; see
[docs/docker.md](docs/docker.md) for how its numbers compare to a native build.

## Commands

### lz

lzbench-style compression benchmark over raw file bytes:

```sh
pqbench lz file.bin -c zstd@3 --samples 10
```

`--json` emits the same report as composable JSON.

### compression

The same codec sweep over the encoded pages of a **NONE-compressed** parquet
file:

```sh
pqbench compression data.parquet --per-column
```

`--json` emits the same report as composable JSON (the per-column rows are
included when `--per-column` is set).

### bytemass

Per-column byte masses — how many on-disk bytes each column takes per row.
Reads only the footer metadata, so it works on any file regardless of
compression. Multiple paths, quoted glob masks, and storage URIs are
aggregated:

```sh
pqbench bytemass data.parquet
pqbench bytemass 'data/part-*.parquet'
pqbench bytemass s3://bucket/table/part-0.parquet   # requires --features aws
```

`--json` emits a flat per-column JSON table (file/row counts plus one record
per column); `--d3` emits a self-contained HTML treemap:

```sh
pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
```

Remote reads fetch the object metadata, the Parquet trailer, and the
serialized footer — never the data pages. `s3://` support is the `aws`
feature; a URI whose backend is not compiled in fails at runtime with the
missing feature named. The library entry point (`bytemass::bytemass`) is
always available and never feature-gated.

### table

Detect the table format, load its metadata, and emit a `pqbench.table`
document. For now only Delta is loaded: the command reads the transaction log
and the active files. A TTY pretty-prints the log; a pipe writes the full
document. `bytemass` understands that document:

```sh
pqbench table ./path/to/table
pqbench table ./path/to/table | pqbench bytemass
pqbench table ./path/to/table | pqbench bytemass --d3 > treemap.html
```

Format detection runs first (`_delta_log` is Delta; `metadata/version-hint.text`
is Iceberg). Iceberg is recognized and rejected until a loader exists. Delta
needs `--features delta` (`delta-s3` for `s3://`).

A producer can hand `table` the same `pqbench.remote-source` document used
elsewhere — one table URI plus optional `AWS_*` credentials — and the table
document carries those credentials to `bytemass`:

```sh
producer | pqbench table | pqbench bytemass
```

### Nested table collections

Analyze tables concurrently while preserving their lake/catalog/schema hierarchy.
`bytemass` detects a `pqbench.collection` document on stdin or as a file:

```bash
pqbench bytemass lake.json --table-jobs 4 --file-jobs 32 \
  --d3 --output-dir reports
producer | pqbench bytemass --d3 --output-dir reports
```

`--json` emits the same tree with a result on each table; `--d3` embeds that
tree in one HTML page: a layer list plus a clickable treemap that drills lake
layers. See [collections](docs/collections.md) for the input format and a
Databricks CLI → `jq` → pqbench example.

### Documents on stdin or a file

`bytemass` and `table` read the `kind` field instead of taking a format flag:

```json
{"kind": "pqbench.remote-source", "version": 1,
 "inputs": ["s3://bucket/table/part-0.parquet"],
 "env": {"AWS_SESSION_TOKEN": "..."}}
```

`pqbench.remote-source` names Parquet objects for `bytemass`, or one table URI
for `table`. `pqbench.table` is the output of `pqbench table`.
`pqbench.collection` is a nested list of tables. `env` is optional, accepts
only `AWS_*` names, and is applied before the read, so a catalog that vends
expiring credentials can pass them through the pipe rather than into your
shell. pqbench keeps no catalog dependency of its own; the
[Unity Catalog example](docker/e2e-lakehouse/README.md) shows a producer.

## Documentation

- [Visual demos](docs/demo.md) — lake treemap, terminal recordings, public samples
- [Collections](docs/collections.md) — lake / catalog / schema input and report
- [Unity Catalog E2E example](docker/e2e-lakehouse/README.md) — a catalog vending
  expiring credentials into `pqbench table`, over rustfs S3
- [Delta tables](docs/delta.md) — log load, `table | bytemass`, limitations
- [Docker](docs/docker.md) — build, run, and publish a container image

## Contributing

PRs welcome. The gate is `make check` (`fmt-check` + `clippy -D warnings` +
`test`) and every change must pass it. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

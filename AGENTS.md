# Project instructions

## Efficient Rust builds

- Reuse build caches. Do not run `cargo clean`, delete `target`, or discard Docker cache volumes unless the user explicitly asks.
- For native builds, use the repository's existing `target/` directory and keep the configured `sccache` wrapper enabled.
- This Docker daemon cannot see host files under `/tmp` or `/private/tmp`. Stage external inputs once under the ignored `.docker-data/` directory so they are available through the repository's shared host path.
- For Rust builds in disposable Docker containers, persist both compiled artifacts and downloaded Cargo dependencies. For Rust 1.91.1 on ARM64, use:

  ```sh
  docker run --rm --platform linux/arm64 \
    -v "$PWD:/src:ro" \
    -v "$PWD/.docker-data/stackexchange-delta:/demo:ro" \
    -v pqbench-cache-target-191:/src/target \
    -v pqbench-cache-registry-191:/usr/local/cargo/registry \
    -v pqbench-cache-git-191:/usr/local/cargo/git \
    -w /src rust:1.91.1-bookworm \
    cargo run --locked --package pqbench-cli -- bytemass /demo/*.parquet --d3
  ```

- Keep cache volume names stable for the same Rust toolchain and platform. Use toolchain-specific names when changing Rust versions to prevent incompatible artifacts from mixing.
- When redirecting generated output, redirect on the host (for example, `> /tmp/pqbench-example.html`) so it does not require a writable source mount.
- Regenerate the terminal preview GIFs with `docs/demos/record.sh`. It requires
  Docker, `asciinema`, and `agg`; the script builds both CLIs with persistent,
  architecture-specific Cargo volumes before recording them.

## Public Parquet and Delta samples

The following anonymous endpoints were verified on 2026-09-19. Prefer these reusable inputs for tests and demos instead of generating large fixtures from scratch. Download or stage them under the ignored `.docker-data/` directory when a command requires local files.

- Tiny Parquet smoke test (public GCS, 1,849 bytes):
  `https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.parquet`
- Official Apache Parquet compatibility fixture (HTTPS, 1,698 bytes):
  `https://raw.githubusercontent.com/apache/parquet-testing/master/data/alltypes_dictionary.parquet`
  Other format and edge-case fixtures live in `https://github.com/apache/parquet-testing/tree/master/data`.
- Medium Parquet demo (public Azure Blob, 7,218,937 bytes):
  `https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_county/year=2000/part-00177-tid-926394737839939592-51ecde30-440a-40fd-9b41-831814678ab5-1919150.c000.snappy.parquet`
- Realistic Parquet visualization demo (NYC yellow taxi, HTTPS, 49,961,641 bytes):
  `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`
- Small anonymous Delta table with three language partitions (public S3, roughly 50 KB):
  `s3://daft-public-datasets/red-pajamas/stackexchange-sample-north-germanic-deltalake`
  HTTPS root: `https://daft-public-datasets.s3.us-west-2.amazonaws.com/red-pajamas/stackexchange-sample-north-germanic-deltalake/`
  First log: `https://daft-public-datasets.s3.us-west-2.amazonaws.com/red-pajamas/stackexchange-sample-north-germanic-deltalake/_delta_log/00000000000000000000.json`

Use the GCS file for a fast happy-path check, Apache fixtures for compatibility and regression coverage, Azure Census for a medium cloud example, NYC Taxi for a meaningful treemap demo, and the Daft S3 table for Delta table checks.

Unity Catalog sample tables such as `samples.nyctaxi`, `samples.tpcds_sf1`, and `samples.tpch` require a Databricks workspace and are not anonymous object URLs. Do not use the formerly public `pandemicdatalake` Bing COVID Parquet URL or `ursa-labs-taxi-data` S3 bucket in unattended tests; live checks returned access errors on 2026-09-19.

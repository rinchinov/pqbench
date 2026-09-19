# Visual demos

This example uses a real public dataset:

- **Single Parquet file:** [NYC TLC yellow-taxi trips for January 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet).

The terminal recording shows discovery through `--help`, normal text output,
JSON output, glob selection, and D3 export. The screenshot shows the resulting
compressed-byte-mass treemap.

## pqbench: one Parquet file

```sh
pqbench bytemass yellow_tripdata_2024-01.parquet
pqbench bytemass 'yellow_tripdata_*.parquet' --json
pqbench bytemass yellow_tripdata_2024-01.parquet --d3 > treemap.html
```

![pqbench CLI walkthrough](images/pqbench-bytemass.gif)

The treemap area represents each column's compressed bytes per physical row:

![NYC Taxi Parquet byte-mass treemap](images/pqbench-bytemass.png)

## Regenerate the terminal recordings

Stage the datasets under `.docker-data/` as documented in
[AGENTS.md](../AGENTS.md), install `asciinema` and `agg`, then run:

```sh
docs/demos/record.sh
```

The script detects the Docker daemon's native ARM64 or AMD64 architecture and
uses persistent, architecture-specific Cargo volumes.

#!/bin/sh
set -eu

run_pqbench() {
    if [ -n "${PQBENCH:-}" ]; then
        "$PQBENCH" "$@"
        return
    fi
    docker run --rm --platform "$PQBENCH_DEMO_PLATFORM" \
        -v "$PWD:/src:ro" \
        -v "$PWD/.docker-data:/src/.docker-data:ro" \
        -v "$PQBENCH_DEMO_TARGET:/src/target" \
        -w /src rust:1.94.1-bookworm \
        target/debug/pqbench "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 2
}

prompt pqbench bytemass --help
run_pqbench bytemass --help
sleep 4

prompt "jq '{lake, catalog: .catalogs[0].name, schemas: [.catalogs[0].schemas[] | {name, tables: [.tables[].name]}]}' docs/demos/lake.json"
jq '{lake, catalog: .catalogs[0].name, schemas: [.catalogs[0].schemas[] |
    {name, tables: [.tables[].name]}]}' docs/demos/lake.json
sleep 4

prompt "pqbench bytemass --collection docs/demos/lake.json --json"
run_pqbench bytemass --collection docs/demos/lake.json --json \
    >.docker-data/pqbench-lake.json
jq '{kind, lake, catalogs: [.catalogs[] | {name, schemas: [.schemas[] |
    {name, tables: [.tables[] | {name, status}]}]}]}' \
    .docker-data/pqbench-lake.json
sleep 4

prompt "pqbench bytemass --collection docs/demos/lake.json --d3 > lake.html"
run_pqbench bytemass --collection docs/demos/lake.json --d3 \
    >.docker-data/pqbench-lake.html
wc -c .docker-data/pqbench-lake.html |
    awk '{ print "wrote lake.html (" $1 " bytes)" }'
sleep 5

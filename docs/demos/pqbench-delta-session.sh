#!/bin/sh
set -eu

pqbench() {
    docker run --rm -i --platform "$PQBENCH_DEMO_PLATFORM" \
        -v "$PWD:/src:ro" \
        -v "$PWD/.docker-data/stackexchange-delta:/demo:ro" \
        -v "$PQBENCH_DEMO_TARGET:/src/target" \
        -w /src rust:1.94.1-bookworm \
        target/debug/pqbench "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 2
}

prompt pqbench table --help
pqbench table --help
sleep 4

prompt pqbench table ./delta-table
pqbench table /demo
sleep 4

prompt pqbench table ./delta-table --version 0
pqbench table /demo --version 0 >.docker-data/pqbench-delta-report.json
sed -n '1,20p' .docker-data/pqbench-delta-report.json
printf '... JSON truncated for preview\n'
sleep 4

prompt "pqbench table ./delta-table | pqbench bytemass --d3 > treemap.html"
pqbench table /demo | pqbench bytemass --d3 \
    >.docker-data/pqbench-delta-treemap.html
wc -c .docker-data/pqbench-delta-treemap.html | awk '{ print "wrote treemap.html (" $1 " bytes)" }'
sleep 5

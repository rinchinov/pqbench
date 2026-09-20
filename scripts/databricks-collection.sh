#!/usr/bin/env bash
# Emit a collection of Delta tables using ambient storage credentials.
# Usage: bash scripts/databricks-collection.sh [catalog ...]
set -euo pipefail

if (($#)); then
  catalogs=$(jq -cn '$ARGS.positional' --args "$@")
else
  catalogs=$(databricks catalogs list -o json | jq -ce 'map(.name)')
fi

# Buffer the complete document: a discovery failure must not emit a partial lake.
collection=$(
  set -e
  printf '%s\n' "$catalogs" | jq -c '.[]' |
    while IFS= read -r catalog_json; do
      catalog=$(jq -r '.' <<< "$catalog_json")
      databricks schemas list "$catalog" -o json | jq -c '.[]' |
        while IFS= read -r schema_json; do
          schema=$(jq -r '.name' <<< "$schema_json")
          databricks tables list "$catalog" "$schema" --omit-columns --omit-properties -o json |
            jq -ce --arg name "$schema" '
              {name: $name, tables: [ .[] |
                select(.data_source_format == "DELTA") |
                if (.storage_location // "") == "" then
                  error("Delta table has no readable storage_location: " + .name)
                else
                  {name, format: "delta", source: {
                    kind: "pqbench.remote-source", version: 1,
                    inputs: [.storage_location]}}
                end
              ]}'
        done | jq -cs --arg name "$catalog" '{name: $name, schemas: .}'
    done | jq -cs '{kind: "pqbench.collection", version: 1, catalogs: .}'
)
printf '%s\n' "$collection"

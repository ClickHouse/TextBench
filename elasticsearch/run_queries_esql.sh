#!/bin/bash

# Usage: ./run_queries_esql.sh [index] [log_file] [query_file]
#   index:      full index name to run against (e.g. otel_logs_1b)
#   log_file:   file to append raw JSON responses to (default: /dev/null)
#   query_file: path to ES|QL query JSON file (default: queries_10_esql.json)
#
# For each query:
#   1. Stop Elasticsearch
#   2. Drop OS filesystem cache
#   3. Start Elasticsearch and wait for it to be ready
#   4. Run the query 3 times — run 1 is cold, runs 2-3 are hot
#   5. Move to next query
#
# The placeholder "otel_logs" in each FROM clause is replaced with the given index.

ES_URL="${ES_URL:-http://localhost:9200}"
INDEX="${1:?Usage: $0 <index> [log_file] [query_file]}"
LOG_FILE="${2:-/dev/null}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY_FILE="${3:-$SCRIPT_DIR/queries_10_esql.json}"

if [[ ! -f "$QUERY_FILE" ]]; then
    echo "Error: query file '$QUERY_FILE' not found." >&2
    exit 1
fi

wait_for_es() {
    local retries=0
    until curl -sf "$ES_URL/_cluster/health?wait_for_status=yellow&timeout=5s" > /dev/null 2>&1; do
        retries=$((retries + 1))
        if [[ $retries -ge 60 ]]; then
            echo "ERROR: Elasticsearch did not start within 60s" >&2
            exit 1
        fi
        sleep 1
    done
}

QUERY_COUNT=$(jq 'length' "$QUERY_FILE")
echo "Running $QUERY_COUNT ES|QL queries against $INDEX (3 runs each — run 1 cold, runs 2-3 hot)"
echo ""

for idx in $(seq 0 $((QUERY_COUNT - 1))); do
    LABEL=$(jq -r ".[$idx].label"       "$QUERY_FILE")
    DESC=$(jq -r  ".[$idx].description" "$QUERY_FILE")
    # Replace the placeholder index name with the actual index
    ESQL=$(jq -r ".[$idx].esql" "$QUERY_FILE" | sed "s/FROM otel_logs/FROM $INDEX/g")
    BODY=$(jq -n --arg q "$ESQL" '{"query": $q}')

    # 1. Stop ES
    sudo systemctl stop elasticsearch

    # 2. Drop OS filesystem cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    # 3. Start ES and wait for ready
    sudo systemctl start elasticsearch
    wait_for_es

    echo "--- $LABEL: $DESC ---"
    for i in $(seq 1 3); do
        RESPONSE=$(curl -sf -X POST "$ES_URL/_query" \
            -H 'Content-Type: application/json' \
            -d "$BODY")
        echo "$RESPONSE" >> "$LOG_FILE"
        TOOK_MS=$(echo "$RESPONSE" | jq -r '.took')
        TOOK_S=$(bc <<< "scale=3; $TOOK_MS / 1000")
        if [[ $i -eq 1 ]]; then
            printf "  Cold  run: %.3f s\n" "$TOOK_S"
        else
            printf "  Hot   run: %.3f s\n" "$TOOK_S"
        fi
    done
    echo ""
done

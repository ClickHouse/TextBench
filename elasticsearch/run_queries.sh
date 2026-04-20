#!/bin/bash

# Usage: ./run_queries.sh [query_file] [index] [log_file]
#   query_file: path to JSON query file (default: queries.json)
#   index:      override the index name from the query file (e.g. otel_logs_1b)
#   log_file:   file to append raw JSON responses to (default: /dev/null)
#
# For each query:
#   1. Stop Elasticsearch
#   2. Drop OS filesystem cache
#   3. Start Elasticsearch and wait for it to be ready
#   4. Run the query 3 times — run 1 is cold, runs 2-3 are hot
#   5. Move to next query

TRIES=3
ES_URL="${ES_URL:-http://localhost:9200}"
QUERY_FILE="${1:-queries.json}"
INDEX_OVERRIDE="${2:-}"
LOG_FILE="${3:-/dev/null}"

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
echo "Running $QUERY_COUNT queries from $QUERY_FILE (${TRIES} runs each — run 1 cold, runs 2-3 hot)"
echo ""

for idx in $(seq 0 $((QUERY_COUNT - 1))); do
    LABEL=$(jq -r ".[$idx].label"       "$QUERY_FILE")
    DESC=$(jq -r  ".[$idx].description" "$QUERY_FILE")
    INDEX="${INDEX_OVERRIDE:-$(jq -r ".[$idx].index" "$QUERY_FILE")}"
    ENDPOINT=$(jq -r ".[$idx].endpoint" "$QUERY_FILE")
    BODY=$(jq -c  ".[$idx].body"        "$QUERY_FILE")

    # 1. Stop ES
    sudo systemctl stop elasticsearch

    # 2. Drop OS filesystem cache
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    # 3. Start ES and wait for ready
    sudo systemctl start elasticsearch
    wait_for_es

    echo "--- $LABEL: $DESC ---"
    for i in $(seq 1 $TRIES); do
        RESPONSE=$(curl -sf -X GET "$ES_URL/$INDEX/$ENDPOINT?request_cache=false" \
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

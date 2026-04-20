#!/bin/bash
# Usage: ./run_query.sh <label> <scale>
#   label: Q1 .. Q10
#   scale: 1b | 10b | 50b
#
# Example: ./run_query.sh Q1 10b
#
# 1. Stop Elasticsearch
# 2. Drop OS filesystem cache
# 3. Start Elasticsearch and wait for ready
# 4. Run query 3 times — run 1 is cold, runs 2-3 are hot

set -euo pipefail

LABEL="${1:?Usage: $0 <label> <scale>}"
SCALE="${2:?Usage: $0 <label> <scale>}"
ES_URL="${ES_URL:-http://localhost:9200}"
INDEX="otel_logs_${SCALE}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY_FILE="$SCRIPT_DIR/queries.json"

BODY=$(jq -c --arg label "$LABEL" '.[] | select(.label == $label) | .body' "$QUERY_FILE")

if [[ -z "$BODY" ]]; then
    echo "Error: query '$LABEL' not found in $QUERY_FILE" >&2
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

# 1. Stop ES
sudo systemctl stop elasticsearch

# 2. Drop OS filesystem cache
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

# 3. Start ES and wait for ready
sudo systemctl start elasticsearch
wait_for_es

echo "$LABEL on $INDEX"
for i in 1 2 3; do
    RESPONSE=$(curl -sf -X GET "$ES_URL/$INDEX/_search?request_cache=false" \
        -H 'Content-Type: application/json' \
        -d "$BODY")
    TOOK_MS=$(echo "$RESPONSE" | jq -r '.took')
    TOOK_S=$(bc <<< "scale=3; $TOOK_MS / 1000")
    if [[ $i -eq 1 ]]; then
        printf "  Cold  run: %.3f s\n" "$TOOK_S"
    else
        printf "  Hot   run: %.3f s\n" "$TOOK_S"
    fi
done

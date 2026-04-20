#!/bin/bash

# Usage: ./benchmark_esql.sh [scale] [index] [result_file]
#   scale:       1b | 10b | 50b  (default: 1b)
#   index:       index to benchmark (default: otel_logs_<scale>)
#   result_file: path to write result JSON (default: results/m6i.8xlarge_<index>_esql.json)
#
# Runs all ES|QL queries (3 runs each), then writes a JSONBench-compatible result file.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALE="${1:-1b}"
ES_URL="${ES_URL:-http://localhost:9200}"
MACHINE="${MACHINE:-m6i.8xlarge}"

case "$SCALE" in
    1b)  DATASET_SIZE=1000000000  ;;
    10b) DATASET_SIZE=10000000000 ;;
    50b) DATASET_SIZE=50000000000 ;;
    *) echo "Unknown scale '$SCALE'. Use: 1b | 10b | 50b" >&2; exit 1 ;;
esac

INDEX="${2:-otel_logs_${SCALE}}"
DEFAULT_RESULT_FILE="$SCRIPT_DIR/results/${MACHINE}_${INDEX}_esql.json"
RESULT_FILE="${3:-$DEFAULT_RESULT_FILE}"

mkdir -p "$(dirname "$RESULT_FILE")"

LOG_FILE="$SCRIPT_DIR/query_log_esql_${SCALE}_$(date +%Y%m%d_%H%M%S).txt"

# --- Gather metadata ---
echo "Collecting cluster metadata..."

ES_VERSION=$(curl -sf "$ES_URL" | python3 -c "import sys,json; print(json.load(sys.stdin)['version']['number'])")
OS_INFO=$(lsb_release -ds 2>/dev/null || uname -sr)
TODAY=$(date +%Y-%m-%d)

NUM_DOCS=$(curl -sf "$ES_URL/$INDEX/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
INDEX_SIZE=$(curl -sf "$ES_URL/_cat/indices/$INDEX?h=pri.store.size&bytes=b" | awk '{s+=$1} END {print s}')

echo "  ES version:   $ES_VERSION"
echo "  Documents:    $NUM_DOCS"
echo "  Index size:   $INDEX_SIZE bytes"
echo ""

# --- Run queries ---
echo "Running ES|QL queries against $INDEX ..."
RAW_LOG_FILE="$SCRIPT_DIR/query_responses_esql_${SCALE}_$(date +%Y%m%d_%H%M%S).jsonl"
"$SCRIPT_DIR/run_queries_esql.sh" "$INDEX" "$RAW_LOG_FILE" 2>&1 | tee "$LOG_FILE"
echo ""

# --- Build result array: [[cold, hot1, hot2], ...] ---
RESULT_ARRAY=$(
    grep -oP '(?:Cold|Hot)\s+run: \K[\d.]+' "$LOG_FILE" | \
    awk '
    BEGIN { i = 0; q = 0; printf "[" }
    {
        times[i % 3] = $1
        i++
        if (i % 3 == 0) {
            if (q > 0) printf ","
            printf "[%s,%s,%s]", times[0], times[1], times[2]
            q++
        }
    }
    END { print "]" }
    '
)

# --- Write JSONBench-compatible result file ---
python3 - <<EOF
import json, sys

result = {
    "system": "Elasticsearch",
    "version": "$ES_VERSION",
    "os": "$OS_INFO",
    "date": "$TODAY",
    "machine": "$MACHINE",
    "tags": ["esql"],
    "dataset_size": $DATASET_SIZE,
    "num_loaded_documents": $NUM_DOCS,
    "total_size": $INDEX_SIZE,
    "result": $RESULT_ARRAY
}

with open("$RESULT_FILE", "w") as f:
    json.dump(result, f, indent=2)
    f.write("\n")

print(json.dumps(result, indent=2))
EOF

echo ""
echo "Result written to $RESULT_FILE"

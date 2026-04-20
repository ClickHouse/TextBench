#!/bin/bash

# Usage: ./disk_usage.sh [scale] [index] [result_file]
#   scale:       1b | 10b | 50b  (default: 1b)
#   index:       index or alias to inspect (default: otel_logs_<scale>)
#   result_file: path to write raw JSON response (default: results/m6i.8xlarge_<index>_disk_usage.json)
#
# Calls POST /<index>/_disk_usage?run_expensive_tasks=true
# WARNING: this is slow — ~1 min/shard. A 50b alias with 250 shards takes ~5+ hours.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALE="${1:-1b}"
ES_URL="${ES_URL:-http://localhost:9200}"
MACHINE="${MACHINE:-m6i.8xlarge}"

INDEX="${2:-otel_logs_${SCALE}}"
DEFAULT_RESULT_FILE="$SCRIPT_DIR/results/${MACHINE}_${INDEX}_disk_usage.json"
RESULT_FILE="${3:-$DEFAULT_RESULT_FILE}"

mkdir -p "$(dirname "$RESULT_FILE")"

echo "Running disk usage analysis for index: $INDEX"
echo "Result will be written to: $RESULT_FILE"
echo "WARNING: This may take several hours for large indices."
echo ""

START=$(date +%s)

curl -sf -X POST "$ES_URL/$INDEX/_disk_usage?run_expensive_tasks=true" \
    -o "$RESULT_FILE"

END=$(date +%s)
ELAPSED=$(( END - START ))

echo ""
echo "Done in ${ELAPSED}s. Result written to $RESULT_FILE"

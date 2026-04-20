#!/bin/bash

# Usage: ./total_size.sh [scale]
#   scale: 1b | 10b | 50b (default: shows all otel_logs* indices)

ES_URL="${ES_URL:-http://localhost:9200}"
PATTERN="${1:+otel_logs*_${1}}"
PATTERN="${PATTERN:-otel_logs*}"

echo "=== Index sizes ==="
curl -s "$ES_URL/_cat/indices/$PATTERN?h=index,store.size,pri.store.size&bytes=b&s=index" \
    | column -t

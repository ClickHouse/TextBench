#!/bin/bash
set -e

# Usage: ./create_indexes.sh [start_file [end_file]]
#   Creates one index per parquet file: otel_logs_part_NNN
#   Then sets up aliases: otel_logs_1b, otel_logs_10b, otel_logs_50b
#
#   start_file  first file number to create (default: 0)
#   end_file    last file number to create inclusive (default: 49)
#
# Each index gets 5 primary shards, no replicas, best_compression,
# sorted by (ServiceName, Timestamp) to match the ClickHouse primary key.

START="${1:-0}"
END="${2:-49}"
ES_URL="${ES_URL:-http://localhost:9200}"
CODEC="${CODEC:-best_compression}"
SHARDS=5

# ---------------------------------------------------------------------------
# Create one index
# ---------------------------------------------------------------------------
create_index() {
    local FILE_NUM="$1"
    local NAME="otel_logs_part_${FILE_NUM}"

    echo "=== Creating $NAME ==="
    curl -sf -X DELETE "$ES_URL/$NAME" > /dev/null 2>&1 || true

    curl -s -X PUT "$ES_URL/$NAME" \
        -H 'Content-Type: application/json' \
        -d "$(cat <<EOF
{
  "settings": {
    "number_of_shards":   $SHARDS,
    "number_of_replicas": 0,
    "refresh_interval":   "30s",
    "codec":              "$CODEC",
    "sort": {
      "field": ["ServiceName", "Timestamp"],
      "order": ["asc", "asc"]
    }
  },
  "mappings": {
    "properties": {
      "Timestamp":          { "type": "date_nanos" },
      "TraceId":            { "type": "keyword" },
      "SpanId":             { "type": "keyword" },
      "TraceFlags":         { "type": "byte" },
      "SeverityText":       { "type": "keyword" },
      "SeverityNumber":     { "type": "byte" },
      "ServiceName":        { "type": "keyword" },
      "Body":               { "type": "text" },
      "ResourceSchemaUrl":  { "type": "keyword" },
      "ResourceAttributes": { "type": "flattened" },
      "ScopeSchemaUrl":     { "type": "keyword" },
      "ScopeName":          { "type": "keyword" },
      "ScopeVersion":       { "type": "keyword" },
      "ScopeAttributes":    { "type": "flattened" },
      "LogAttributes":      { "type": "flattened" }
    }
  }
}
EOF
)" | python3 -m json.tool --no-indent
    echo ""
}

# ---------------------------------------------------------------------------
# Create indices
# ---------------------------------------------------------------------------
for i in $(seq "$START" "$END"); do
    FILE_NUM=$(printf "%03d" "$i")
    create_index "$FILE_NUM"
done

# ---------------------------------------------------------------------------
# Aliases — rebuild based on all existing otel_logs_part_* indices
# ---------------------------------------------------------------------------
echo "=== Setting up aliases ==="

build_alias_actions() {
    local alias_name="$1"
    local from="$2"
    local to="$3"
    local actions=""
    for i in $(seq "$from" "$to"); do
        FILE_NUM=$(printf "%03d" "$i")
        INDEX="otel_logs_part_${FILE_NUM}"
        HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$ES_URL/$INDEX")
        if [[ "$HTTP_STATUS" == "200" ]]; then
            actions="${actions}{\"add\":{\"index\":\"$INDEX\",\"alias\":\"$alias_name\"}},"
        fi
    done
    echo "${actions%,}"  # strip trailing comma
}

for alias_def in "otel_logs_1b:0:0" "otel_logs_10b:0:9" "otel_logs_50b:0:49"; do
    ALIAS="${alias_def%%:*}"
    REST="${alias_def#*:}"
    FROM="${REST%%:*}"
    TO="${REST##*:}"

    ACTIONS=$(build_alias_actions "$ALIAS" "$FROM" "$TO")
    if [[ -z "$ACTIONS" ]]; then
        echo "  $ALIAS: no indices found, skipping"
        continue
    fi

    # Remove old alias first, then re-add
    curl -s -X DELETE "$ES_URL/*/_alias/$ALIAS" > /dev/null 2>&1 || true
    curl -s -X POST "$ES_URL/_aliases" \
        -H 'Content-Type: application/json' \
        -d "{\"actions\":[${ACTIONS}]}" | python3 -m json.tool --no-indent
    echo "  $ALIAS → parts $(printf '%03d' "$FROM")..$(printf '%03d' "$TO")"
done

echo ""
echo "Done."

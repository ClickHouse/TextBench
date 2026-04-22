#!/bin/bash
set -e

# Creates a single index for the storage optimisation experiment.
#
# v2 — light optimisation (text field only):
#   - Body: norms=false, index_options=docs
#   - everything else identical to v1 baseline

INDEX="${1:-otel_logs_1b_v2}"
ES_URL="${ES_URL:-http://localhost:9200}"
CODEC="${CODEC:-best_compression}"
SHARDS=5

echo "=== Creating $INDEX ==="
curl -sf -X DELETE "$ES_URL/$INDEX" > /dev/null 2>&1 || true

curl -s -X PUT "$ES_URL/$INDEX" \
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
      "Body":               { "type": "text", "index_options": "docs", "norms": false },
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
echo "Done. Index $INDEX created."

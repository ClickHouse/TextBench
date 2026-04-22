#!/bin/bash
set -e

# Creates a single index for the storage optimisation experiment.
#
# v3 — aggressive optimisation (all fields):
#   - keyword fields not used for filtering: index=false
#     (TraceId, SpanId, ResourceSchemaUrl, ScopeSchemaUrl, ScopeName, ScopeVersion)
#   - keyword fields used for filtering: index_options=docs, norms=false
#     (ServiceName, SeverityText)
#   - Body: index_options=docs, norms=false
#   - flattened fields not queried: index=false
#     (ResourceAttributes, ScopeAttributes, LogAttributes)

INDEX="${1:-otel_logs_1b_v3}"
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
      "TraceId":            { "type": "keyword",   "index": false },
      "SpanId":             { "type": "keyword",   "index": false },
      "TraceFlags":         { "type": "byte" },
      "SeverityText":       { "type": "keyword",   "index_options": "docs", "norms": false },
      "SeverityNumber":     { "type": "byte" },
      "ServiceName":        { "type": "keyword",   "index_options": "docs", "norms": false },
      "Body":               { "type": "text",      "index_options": "docs", "norms": false },
      "ResourceSchemaUrl":  { "type": "keyword",   "index": false },
      "ResourceAttributes": { "type": "flattened", "index": false },
      "ScopeSchemaUrl":     { "type": "keyword",   "index": false },
      "ScopeName":          { "type": "keyword",   "index": false },
      "ScopeVersion":       { "type": "keyword",   "index": false },
      "ScopeAttributes":    { "type": "flattened", "index": false },
      "LogAttributes":      { "type": "flattened", "index": false }
    }
  }
}
EOF
)" | python3 -m json.tool --no-indent

echo ""
echo "Done. Index $INDEX created."

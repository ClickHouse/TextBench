#!/bin/bash

# Usage: ./drop_indexes.sh <scale> [--with-ngram]
#   scale: 1b | 10b | 50b

ES_URL="${ES_URL:-http://localhost:9200}"
SCALE="${1:-1b}"
WITH_NGRAM=false
[[ "${2:-}" == "--with-ngram" ]] && WITH_NGRAM=true

for NAME in otel_logs_${SCALE} otel_logs_no_source_${SCALE}; do
    echo "Dropping $NAME ..."
    curl -s -X DELETE "$ES_URL/$NAME" | python3 -m json.tool --no-indent
done

if [[ "$WITH_NGRAM" == "true" ]]; then
    echo "Dropping otel_logs_ngram_${SCALE} ..."
    curl -s -X DELETE "$ES_URL/otel_logs_ngram_${SCALE}" | python3 -m json.tool --no-indent
fi

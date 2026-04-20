#!/bin/bash
set -e

# Usage: ./load_data.sh [start_file [end_file]]
#   Ingests parquet files into per-file indices: otel_logs_part_NNN
#   File N+1 is downloaded in the background while file N is being ingested.
#
#   start_file  first file number to ingest (default: 0)
#   end_file    last file number to ingest inclusive (default: 49)
#
# Env vars:
#   PROCESSES    parallel processes per file (default: 16)
#   BULK_WORKERS bulk HTTP threads per process (default: 4)

FILE_FIRST="${1:-0}"
FILE_LAST="${2:-49}"
ES_URL="${ES_URL:-http://localhost:9200}"
S3_BASE="https://public-pme.s3.eu-west-3.amazonaws.com/text_bench"
PROCESSES="${PROCESSES:-16}"
BULK_WORKERS="${BULK_WORKERS:-4}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Files: $FILE_FIRST to $FILE_LAST  Processes: $PROCESSES  Bulk workers: $BULK_WORKERS"
echo ""

# ---------------------------------------------------------------------------
# Check all target indices exist before starting
# ---------------------------------------------------------------------------
for i in $(seq "$FILE_FIRST" "$FILE_LAST"); do
    FILE_NUM=$(printf "%03d" "$i")
    INDEX="otel_logs_part_${FILE_NUM}"
    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$ES_URL/$INDEX")
    if [[ "$HTTP_STATUS" != "200" ]]; then
        echo "Error: index '$INDEX' not found (HTTP $HTTP_STATUS). Run ./create_indexes.sh $FILE_FIRST $FILE_LAST first." >&2
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------
download_file() {
    local FILE_NUM="$1"
    local TMP_FILE="/tmp/part_${FILE_NUM}.parquet"
    local S3_URI="s3://public-pme/text_bench/part_${FILE_NUM}.parquet"
    local S3_URL="$S3_BASE/part_${FILE_NUM}.parquet"

    if [[ -f "$TMP_FILE" ]]; then
        echo "Download part_${FILE_NUM}: skipped (already exists, $(du -sh "$TMP_FILE" | cut -f1))"
        return
    fi

    local DL_START DL_END
    DL_START=$(date +%s)
    if aws s3 cp --no-sign-request --no-progress "$S3_URI" "$TMP_FILE" 2>/dev/null; then
        :
    elif command -v aria2c &>/dev/null; then
        aria2c -x 16 -s 16 --dir=/tmp --out="part_${FILE_NUM}.parquet" "$S3_URL" > /dev/null
    else
        wget -q -O "$TMP_FILE" "$S3_URL"
    fi
    DL_END=$(date +%s)
    echo "Download part_${FILE_NUM}: $((DL_END - DL_START))s  ($(du -sh "$TMP_FILE" | cut -f1))"
}

# ---------------------------------------------------------------------------
# Ingest loop — pipeline: download N+1 while ingesting N
# ---------------------------------------------------------------------------
source "$SCRIPT_DIR/venv/bin/activate"

TOTAL_START=$(date +%s)
NEXT_DL_PID=""

for i in $(seq "$FILE_FIRST" "$FILE_LAST"); do
    FILE_NUM=$(printf "%03d" "$i")
    NEXT_NUM=$(printf "%03d" "$((i + 1))")
    TMP_FILE="/tmp/part_${FILE_NUM}.parquet"
    INDEX="otel_logs_part_${FILE_NUM}"

    # Wait for this file's download (background or foreground)
    if [[ -n "$NEXT_DL_PID" ]]; then
        echo "--- File $((i - FILE_FIRST + 1))/$((FILE_LAST - FILE_FIRST + 1)): waiting for background download ---"
        wait "$NEXT_DL_PID"
    else
        echo "--- File $((i - FILE_FIRST + 1))/$((FILE_LAST - FILE_FIRST + 1)): downloading part_${FILE_NUM}.parquet ---"
        download_file "$FILE_NUM"
    fi

    # Start downloading next file in background
    if [[ $i -lt $FILE_LAST ]]; then
        echo "--- File $((i - FILE_FIRST + 2))/$((FILE_LAST - FILE_FIRST + 1)): starting background download of part_${NEXT_NUM}.parquet ---"
        download_file "$NEXT_NUM" &
        NEXT_DL_PID=$!
    else
        NEXT_DL_PID=""
    fi

    # Pre-ingest settings
    curl -s -X PUT "$ES_URL/$INDEX/_settings" \
        -H 'Content-Type: application/json' \
        -d '{
          "index.refresh_interval":       "-1",
          "index.translog.durability":    "async",
          "index.translog.sync_interval": "120s"
        }' > /dev/null

    # Ingest
    echo "--- File $((i - FILE_FIRST + 1))/$((FILE_LAST - FILE_FIRST + 1)): ingesting part_${FILE_NUM}.parquet into $INDEX ---"
    INGEST_START=$(date +%s)
    python3 "$SCRIPT_DIR/ingest.py" \
        --index        "$INDEX"        \
        --files        1               \
        --start-file   "$i"            \
        --processes    "$PROCESSES"    \
        --bulk-workers "$BULK_WORKERS" \
        --local-dir    /tmp
    INGEST_END=$(date +%s)
    echo "Ingest: $((INGEST_END - INGEST_START))s"

    # Post-ingest settings
    curl -s -X PUT "$ES_URL/$INDEX/_settings" \
        -H 'Content-Type: application/json' \
        -d '{
          "index.refresh_interval":       "30s",
          "index.translog.durability":    "request",
          "index.translog.sync_interval": "5s",
          "index.blocks.write":           true
        }' > /dev/null

    rm -f "$TMP_FILE"
    echo ""
done

TOTAL_END=$(date +%s)
echo "Total elapsed: $((TOTAL_END - TOTAL_START))s"
echo "Done."

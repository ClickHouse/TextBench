#!/bin/bash
set -e

# Ingest worker — atomically claims file numbers from a shared queue.
# Run multiple instances in parallel; add more at any time.
#
# Usage: nohup bash worker.sh > /tmp/worker_N.log 2>&1 &
#
# Queue file: /tmp/ingest_queue.txt  (one file number per line, e.g. 001)
# Lock file:  /tmp/ingest_queue.lock
#
# To create the queue:
#   seq 1 49 | awk '{printf "%03d\n", $1}' > /tmp/ingest_queue.txt

QUEUE="/tmp/ingest_queue.txt"
LOCK="/tmp/ingest_queue.lock"
ES_URL="${ES_URL:-http://localhost:9200}"
PROCESSES="${PROCESSES:-16}"
BULK_WORKERS="${BULK_WORKERS:-4}"
S3_BASE="https://public-pme.s3.eu-west-3.amazonaws.com/text_bench"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/venv/bin/activate"

WORKER_ID="$$"
echo "[worker $WORKER_ID] started"

# ---------------------------------------------------------------------------
# Claim the next file number from the queue (atomic via flock)
# ---------------------------------------------------------------------------
claim_next() {
    (
        flock -x 200
        FILE_NUM=$(head -1 "$QUEUE" 2>/dev/null)
        if [[ -z "$FILE_NUM" ]]; then
            echo ""
            exit 0
        fi
        # Remove the claimed line
        tail -n +2 "$QUEUE" > "${QUEUE}.tmp" && mv "${QUEUE}.tmp" "$QUEUE"
        echo "$FILE_NUM"
    ) 200>"$LOCK"
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
while true; do
    FILE_NUM=$(claim_next)
    if [[ -z "$FILE_NUM" ]]; then
        echo "[worker $WORKER_ID] queue empty, exiting"
        exit 0
    fi

    INDEX="otel_logs_part_${FILE_NUM}"
    TMP_FILE="/tmp/part_${FILE_NUM}.parquet"
    echo "[worker $WORKER_ID] claimed part_${FILE_NUM} → $INDEX"

    # Create index if it doesn't exist
    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$ES_URL/$INDEX")
    if [[ "$HTTP_STATUS" != "200" ]]; then
        echo "[worker $WORKER_ID] creating index $INDEX"
        bash "$SCRIPT_DIR/create_indexes.sh" "$((10#$FILE_NUM))" "$((10#$FILE_NUM))"
    fi

    # Download
    if [[ ! -f "$TMP_FILE" ]]; then
        echo "[worker $WORKER_ID] downloading part_${FILE_NUM}.parquet"
        DL_START=$(date +%s)
        S3_URI="s3://public-pme/text_bench/part_${FILE_NUM}.parquet"
        S3_URL="$S3_BASE/part_${FILE_NUM}.parquet"
        if aws s3 cp --no-sign-request --no-progress "$S3_URI" "$TMP_FILE" 2>/dev/null; then
            :
        elif command -v aria2c &>/dev/null; then
            aria2c -x 16 -s 16 --dir=/tmp --out="part_${FILE_NUM}.parquet" "$S3_URL" > /dev/null
        else
            wget -q -O "$TMP_FILE" "$S3_URL"
        fi
        echo "[worker $WORKER_ID] download done in $(($(date +%s) - DL_START))s ($(du -sh "$TMP_FILE" | cut -f1))"
    else
        echo "[worker $WORKER_ID] part_${FILE_NUM}.parquet already on disk, skipping download"
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
    echo "[worker $WORKER_ID] ingesting part_${FILE_NUM}.parquet"
    INGEST_START=$(date +%s)
    python3 "$SCRIPT_DIR/ingest.py" \
        --index        "$INDEX"        \
        --files        1               \
        --start-file   "$((10#$FILE_NUM))" \
        --processes    "$PROCESSES"    \
        --bulk-workers "$BULK_WORKERS" \
        --local-dir    /tmp
    INGEST_ELAPSED=$(($(date +%s) - INGEST_START))
    echo "[worker $WORKER_ID] ingest done in ${INGEST_ELAPSED}s"

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
    echo "[worker $WORKER_ID] done with part_${FILE_NUM}"
    echo ""
done

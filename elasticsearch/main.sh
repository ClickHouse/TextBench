#!/bin/bash
set -e

# Full benchmark orchestration for Elasticsearch.
#
# Usage: ./main.sh [scale] [output_prefix]
#   scale:         1b | 10b | 50b | all  (default: asks interactively)
#   output_prefix: prefix for result files (default: _m6i.8xlarge)
#
# For each scale the script:
#   1. Starts Elasticsearch
#   2. Creates otel_logs (standard) + otel_logs_ngram (trigram) indexes
#   3. Ingests data into both indexes
#   4. Restarts ES (cold-start simulation)
#   5. Runs benchmark queries (3 runs each)
#   6. Records index sizes
#   7. Drops indexes

DEFAULT_CHOICE=ask
CHOICE="${1:-$DEFAULT_CHOICE}"
OUTPUT_PREFIX="${2:-_m6i.8xlarge}"

if [ "$CHOICE" = "ask" ]; then
    echo "Select the dataset size to benchmark:"
    echo "1) 1b  — 1 Parquet file  (~1B rows)"
    echo "2) 10b — 10 Parquet files (~10B rows)"
    echo "3) 50b — all 50 files     (~50B rows)"
    echo "4) all — run 1b → 10b → 50b"
    read -rp "Enter choice [1-4]: " CHOICE
fi

./install.sh

benchmark() {
    local scale=$1   # 1b | 10b | 50b
    local suffix="_${scale}"

    echo ""
    echo "========================================"
    echo "  SCALE: $scale"
    echo "========================================"

    ./start.sh
    ./create_indexes.sh "$scale"

    # Ingest standard index
    ./load_data.sh "$scale" "otel_logs"

    # Ingest ngram index (same data, different analyzer — separate ingest pass)
    ./load_data.sh "$scale" "otel_logs_ngram"

    # Record index sizes (after force merge, before restart)
    ./total_size.sh | tee "${OUTPUT_PREFIX}_es_${scale}.index_size"

    # Restart to simulate cold start / clear in-memory state
    echo ""
    echo "=== Restarting Elasticsearch (cold start) ==="
    sudo systemctl restart elasticsearch
    sleep 10
    ./start.sh   # wait until healthy

    # Run benchmark
    ./benchmark.sh "" "${OUTPUT_PREFIX}_es_${scale}.results_runtime"

    ./drop_indexes.sh
}

case $CHOICE in
    2) benchmark 10b ;;
    3) benchmark 50b ;;
    4)
        benchmark 1b
        benchmark 10b
        benchmark 50b
        ;;
    *)
        benchmark 1b
        ;;
esac

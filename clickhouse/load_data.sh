#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 6 || $# -gt 7 ]]; then
    echo "Usage: $0 <DATA_DIRECTORY> <DB_NAME> <TABLE_NAME> <MAX_FILES> <SUCCESS_LOG> <ERROR_LOG> [PARALLEL_WORKERS]"
    exit 1
fi

DATA_DIRECTORY="$1"
DB_NAME="$2"
TABLE_NAME="$3"
MAX_FILES="$4"
SUCCESS_LOG="$5"
ERROR_LOG="$6"
PARALLEL_WORKERS="${7:-1}"

[[ ! -d "$DATA_DIRECTORY" ]] && { echo "Error: Data directory '$DATA_DIRECTORY' does not exist."; exit 1; }
[[ ! "$MAX_FILES" =~ ^[0-9]+$ ]] && { echo "Error: MAX_FILES must be a positive integer."; exit 1; }
[[ ! "$PARALLEL_WORKERS" =~ ^[0-9]+$ ]] && { echo "Error: PARALLEL_WORKERS must be a positive integer."; exit 1; }
[[ "$PARALLEL_WORKERS" -lt 1 ]] && { echo "Error: PARALLEL_WORKERS must be >= 1."; exit 1; }

touch "$SUCCESS_LOG" "$ERROR_LOG"

echo "=== Load started at $(date '+%Y-%m-%d %H:%M:%S') ==="
echo "Database:         $DB_NAME"
echo "Table:            $TABLE_NAME"
echo "Data directory:   $DATA_DIRECTORY"
echo "Max files:        $MAX_FILES"
echo "Parallel workers: $PARALLEL_WORKERS"
echo

mapfile -t files < <(
    find "$DATA_DIRECTORY" -maxdepth 1 -type f -name 'part_*.parquet' \
    | LC_ALL=C sort \
    | head -n "$MAX_FILES"
)

if [[ ${#files[@]} -eq 0 ]]; then
    echo "Error: No parquet files found in '$DATA_DIRECTORY'." | tee -a "$ERROR_LOG"
    exit 1
fi

echo "Files to process (${#files[@]}):"
for f in "${files[@]}"; do
    echo "  - $(basename "$f")"
done
echo

completed=0
failed=0

load_one_file() {
    local file="$1"
    local fname
    fname=$(basename "$file")

    echo "[$(date '+%H:%M:%S')] START  $fname"

    if clickhouse client \
        --async_insert=0 \
        --query "INSERT INTO ${DB_NAME}.${TABLE_NAME} FORMAT Parquet" \
        < "$file"; then
        echo "[$(date '+%H:%M:%S')] DONE   $fname"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Successfully imported $file." >> "$SUCCESS_LOG"
        return 0
    else
        echo "[$(date '+%H:%M:%S')] FAIL   $fname"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Import failed for $file." >> "$ERROR_LOG"
        return 1
    fi
}

for file in "${files[@]}"; do
    load_one_file "$file" &

    while [[ $(jobs -p -r | wc -l) -ge $PARALLEL_WORKERS ]]; do
        if wait -n; then
            completed=$((completed + 1))
        else
            failed=$((failed + 1))
        fi
        echo "Progress: $completed completed, $failed failed"
    done
done

while [[ $(jobs -p -r | wc -l) -gt 0 ]]; do
    if wait -n; then
        completed=$((completed + 1))
    else
        failed=$((failed + 1))
    fi
    echo "Progress: $completed completed, $failed failed"
done

echo
echo "=== Load finished at $(date '+%Y-%m-%d %H:%M:%S') ==="
echo "Total files: ${#files[@]}"
echo "Completed:   $completed"
echo "Failed:      $failed"
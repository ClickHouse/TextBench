#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 6 || $# -gt 7 ]]; then
    echo "Usage: $0 <DATA_DIRECTORY> <DB_NAME> <TABLE_NAME> <MAX_FILES> <SUCCESS_LOG> <ERROR_LOG> [PARALLEL_WORKERS]"
    echo
    echo "Remote ClickHouse Cloud connection (optional env vars):"
    echo "  export FQDN=<host>        e.g. wjgkgcnnmt.us-east-2.aws.clickhouse-staging.com"
    echo "  export PASSWORD=<password>"
    echo "  export CH_USER=<user>     (default: default)"
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

# ---------------------------------------------------------------------------
# Build ClickHouse connection flags.
# When FQDN and PASSWORD are set, connect to ClickHouse Cloud over TLS.
# Otherwise fall back to the default localhost connection.
# ---------------------------------------------------------------------------
CH_OPTS=()
if [[ -n "${FQDN:-}" && -n "${PASSWORD:-}" ]]; then
    CH_USER="${CH_USER:-default}"
    CH_OPTS=(
        --host="$FQDN"
        --user="$CH_USER"
        --password="$PASSWORD"
        --secure
        --enable_full_text_index=1
    )
fi

touch "$SUCCESS_LOG" "$ERROR_LOG"

echo "=== Load started at $(date '+%Y-%m-%d %H:%M:%S') ==="
echo "Database:         $DB_NAME"
echo "Table:            $TABLE_NAME"
echo "Data directory:   $DATA_DIRECTORY"
echo "Max files:        $MAX_FILES"
echo "Parallel workers: $PARALLEL_WORKERS"
if [[ ${#CH_OPTS[@]} -gt 0 ]]; then
    echo "Target:           ${CH_USER:-default}@${FQDN} (secure)"
else
    echo "Target:           localhost (default)"
fi
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

    if clickhouse client "${CH_OPTS[@]}" \
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

# Export so subshells spawned by & can see CH_OPTS
export -f load_one_file
export CH_OPTS DB_NAME TABLE_NAME SUCCESS_LOG ERROR_LOG

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
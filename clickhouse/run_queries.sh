#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Defaults / config
# ============================================================
DRY_RUN=0
RUNS_PER_QUERY=3
SYSTEM="ClickHouse"
OS_NAME="$(grep '^PRETTY_NAME=' /etc/os-release 2>/dev/null | cut -d= -f2- | tr -d '"' || true)"
OS_NAME="${OS_NAME:-$(uname -sr)}"

CLIENT="clickhouse-client"

QUERY_FILE=""
DATABASE=""

MACHINE=""
DATASET_SIZE=""
TOTAL_SIZE=""
DATA_SIZE=0
TEXT_INDEX_SIZE=""
CLUSTER_SIZE=1
PARALLEL_REPLICAS=0
COMMENT=""
TAGS='["C++","column-oriented","ClickHouse","managed","aws"]'
LOAD_TIME=0
PROPRIETARY="yes"
TUNED="no"

# ============================================================
# Remote connection (env-var driven)
# When FQDN and PASSWORD are set, connect to ClickHouse Cloud
# over TLS instead of localhost.  CH_USER is an optional override
# (default: default).
# ============================================================
CH_OPTS=()
REMOTE=0

if [[ -n "${FQDN:-}" && -n "${PASSWORD:-}" ]]; then
    CH_USER="${CH_USER:-default}"
    CH_OPTS=(
        --host="$FQDN"
        --user="$CH_USER"
        --password="$PASSWORD"
        --secure
    )
    REMOTE=1
fi

# ============================================================
# Logging
# ============================================================
timestamp() {
    date +"%Y-%m-%d %H:%M:%S"
}

log() {
    echo "[$(timestamp)] [INFO] $*" >&2
}

warn() {
    echo "[$(timestamp)] [WARN] $*" >&2
}

error() {
    echo "[$(timestamp)] [ERROR] $*" >&2
}

die() {
    error "$*"
    exit 1
}

# ============================================================
# Usage
# ============================================================
usage() {
    cat >&2 <<'EOF'
Usage:
  ./run_queries.sh --query-file FILE --database DB [options]

Mandatory:
  --query-file FILE         SQL file containing the queries
  --database DB             Database name passed to clickhouse-client -d

Optional:
  --dry-run                 Print each query instead of executing it
  --runs N                  Number of runs per query (default: 3)
  --machine TEXT            Machine description
  --dataset-size N          Dataset size
  --total-size N            Total size
  --data-size N             Data size
  --text-index-size N       Text index size
  --client PATH             clickhouse-client binary (default: clickhouse-client)
  --parallel-replicas 0|1   Enable parallel replicas (default: 0)
  --cluster-size N          Number of nodes for max_parallel_replicas (default: 1)
  --comment TEXT            Free-text comment included in JSON output
  --tags JSON               JSON array string of tags (e.g. '["cloud","aws"]')
  --load-time N             Load time in seconds (default: omitted)
  --proprietary yes|no      Proprietary flag in JSON output
  --tuned yes|no            Tuned flag in JSON output

Remote ClickHouse Cloud (env vars, no flags needed):
  export FQDN=<host>        e.g. wjgkgcnnmt.us-east-2.aws.clickhouse-staging.com
  export PASSWORD=<password>
  export CH_USER=<user>     (default: default)

  When FQDN+PASSWORD are set the script connects over TLS and skips the
  local restart-and-drop-caches step (not applicable to a managed service).

Examples:
  ./run_queries.sh --query-file queries_10B.sql --database logs --dry-run

  ./run_queries.sh \
    --query-file queries_10B.sql \
    --database logs \
    --machine "m6i.8xlarge, 10000gib gp3" \
    --dataset-size 1000000000 \
    --total-size 99560268152 \
    --data-size 99068986216 \
    --text-index-size 491281201
EOF
}

# ============================================================
# Argument parsing
# ============================================================
while [[ $# -gt 0 ]]; do
    case "$1" in
        --query-file)
            QUERY_FILE="${2:-}"
            shift 2
            ;;
        --database)
            DATABASE="${2:-}"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        --runs)
            RUNS_PER_QUERY="${2:-}"
            shift 2
            ;;
        --machine)
            MACHINE="${2:-}"
            shift 2
            ;;
        --dataset-size)
            DATASET_SIZE="${2:-}"
            shift 2
            ;;
        --total-size)
            TOTAL_SIZE="${2:-}"
            shift 2
            ;;
        --data-size)
            DATA_SIZE="${2:-}"
            shift 2
            ;;
        --text-index-size)
            TEXT_INDEX_SIZE="${2:-}"
            shift 2
            ;;
        --client)
            CLIENT="${2:-}"
            shift 2
            ;;
        --parallel-replicas)
            PARALLEL_REPLICAS="${2:-}"
            shift 2
            ;;
        --cluster-size)
            CLUSTER_SIZE="${2:-}"
            shift 2
            ;;
        --comment)
            COMMENT="${2:-}"
            shift 2
            ;;
        --tags)
            TAGS="${2:-}"
            shift 2
            ;;
        --load-time)
            LOAD_TIME="${2:-}"
            shift 2
            ;;
        --proprietary)
            PROPRIETARY="${2:-}"
            shift 2
            ;;
        --tuned)
            TUNED="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "Unknown argument: $1"
            ;;
    esac
done

[[ -n "$QUERY_FILE" ]] || die "--query-file is mandatory"
[[ -f "$QUERY_FILE" ]] || die "Query file not found: $QUERY_FILE"
[[ -n "$DATABASE" ]] || die "--database is mandatory"
[[ "$RUNS_PER_QUERY" =~ ^[0-9]+$ ]] || die "--runs must be a positive integer"
[[ "$RUNS_PER_QUERY" -ge 1 ]] || die "--runs must be >= 1"
[[ "$PARALLEL_REPLICAS" =~ ^[01]$ ]] || die "--parallel-replicas must be 0 or 1"
if [[ -n "$CLUSTER_SIZE" ]]; then
    [[ "$CLUSTER_SIZE" =~ ^[0-9]+$ && "$CLUSTER_SIZE" -ge 1 ]] || die "--cluster-size must be a positive integer"
fi
# When parallel replicas are enabled, default cluster-size to 1 if not set
[[ "$PARALLEL_REPLICAS" == "1" && -z "$CLUSTER_SIZE" ]] && CLUSTER_SIZE=1

# Always build comment, appending parallel_replicas flag
if [[ -n "$COMMENT" ]]; then
    COMMENT="${COMMENT} (enable_parallel_replicas=${PARALLEL_REPLICAS})"
else
    COMMENT="(enable_parallel_replicas=${PARALLEL_REPLICAS})"
fi

# ============================================================
# Helpers
# ============================================================
trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

json_escape() {
    local s="$1"
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    s="${s//$'\n'/\\n}"
    s="${s//$'\r'/\\r}"
    s="${s//$'\t'/\\t}"
    printf '%s' "$s"
}

wait_for_clickhouse() {
    local max_attempts=60
    local attempt

    log "Waiting for ClickHouse to become ready..."

    for ((attempt=1; attempt<=max_attempts; attempt++)); do
        if "$CLIENT" "${CH_OPTS[@]}" -d "$DATABASE" -q "SELECT 1" >/dev/null 2>&1; then
            log "ClickHouse is ready."
            return 0
        fi

        log "ClickHouse not ready yet (attempt $attempt/$max_attempts), sleeping 1s..."
        sleep 1
    done

    die "ClickHouse did not become ready within ${max_attempts} seconds."
}

restart_and_drop_caches() {
    if [[ "$REMOTE" == "1" ]]; then
        log "Remote mode: skipping restart_and_drop_caches (not applicable to ClickHouse Cloud)."
        return 0
    fi

    log "Starting restart_and_drop_caches()"

    log "Stopping ClickHouse..."
    sudo clickhouse stop
    log "ClickHouse stopped."

    log "Dropping Linux page cache..."
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    log "Linux page cache dropped."

    log "Starting ClickHouse..."
    sudo clickhouse start
    log "ClickHouse start command issued."

    wait_for_clickhouse

    log "restart_and_drop_caches() completed successfully."
}

extract_queries() {
    local file="$1"

    log "Extracting queries from file: $file"

    awk '
        BEGIN { RS=";"; ORS="" }
        {
            gsub(/\r/, "", $0)

            n = split($0, lines, "\n")
            out = ""

            for (i = 1; i <= n; i++) {
                if (lines[i] !~ /^[[:space:]]*--/) {
                    out = out lines[i] "\n"
                }
            }

            sub(/^[[:space:]\n]+/, "", out)
            sub(/[[:space:]\n]+$/, "", out)

            if (out != "") {
                printf "%s%c", out, 0
            }
        }
    ' "$file"
}

get_version() {
    if [[ "$DRY_RUN" == "1" ]]; then
        warn "DRY_RUN=1 -> using placeholder version."
        printf 'dry-run'
        return 0
    fi

    log "Fetching ClickHouse version via SELECT version()..."
    "$CLIENT" "${CH_OPTS[@]}" -d "$DATABASE" -q "SELECT version()" | head -n1 | tr -d '\r'
}

get_date_str() {
    date +%F
}

print_query_block() {
    local query_no="$1"
    local total_queries="$2"
    local query="$3"

    echo "============================================================" >&2
    echo "QUERY $query_no/$total_queries" >&2
    echo "============================================================" >&2
    printf '%s\n' "$query" >&2
    echo "============================================================" >&2
}

run_query() {
    local query="$1"
    local run_no="$2"
    local tmp_stdout
    local tmp_stderr
    local elapsed

    if [[ "$DRY_RUN" == "1" ]]; then
        log "DRY_RUN=1 -> would execute query run $run_no against database '$DATABASE'"
        echo "------------------------------------------------------------" >&2
        echo "DRY RUN - DATABASE: $DATABASE - RUN: $run_no" >&2
        echo "------------------------------------------------------------" >&2
        printf '%s\n' "$query" >&2
        echo "------------------------------------------------------------" >&2
        printf '0.000'
        return 0
    fi

    log "Executing query run $run_no against database '$DATABASE'..."

    tmp_stdout="$(mktemp)"
    tmp_stderr="$(mktemp)"

    if ! "$CLIENT" "${CH_OPTS[@]}" -d "$DATABASE" --time -q "$query" >"$tmp_stdout" 2>"$tmp_stderr"; then
        error "Query run $run_no failed."
        error "Query was:"
        printf '%s\n' "$query" >&2
        error "Client stderr:"
        cat "$tmp_stderr" >&2
        rm -f "$tmp_stdout" "$tmp_stderr"
        exit 1
    fi

    echo "-------------------- RESULT (run $run_no) --------------------" >&2
    if [[ -s "$tmp_stdout" ]]; then
        cat "$tmp_stdout" >&2
    else
        echo "[no rows returned]" >&2
    fi
    echo "-------------------------------------------------------------" >&2

    elapsed="$(tail -n 1 "$tmp_stderr" | tr -d '[:space:]')"

    if [[ ! "$elapsed" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        error "Failed to parse runtime from clickhouse-client output."
        error "Raw stderr was:"
        cat "$tmp_stderr" >&2
        rm -f "$tmp_stdout" "$tmp_stderr"
        exit 1
    fi

    rm -f "$tmp_stdout" "$tmp_stderr"

    log "Run $run_no completed in ${elapsed}s."
    printf '%s' "$elapsed"
}


# ============================================================
# Main
# ============================================================
log "Benchmark script started."
log "Query file    : $QUERY_FILE"
log "Database      : $DATABASE"
log "Dry run       : $DRY_RUN"
log "Runs/query    : $RUNS_PER_QUERY"
log "Client        : $CLIENT"

if [[ "$REMOTE" == "1" ]]; then
    log "Target        : ${CH_USER:-default}@${FQDN} (ClickHouse Cloud / TLS)"
else
    log "Target        : localhost (default)"
fi

if [[ -n "$MACHINE" ]]; then log "Machine       : $MACHINE"; fi
if [[ -n "$DATASET_SIZE" ]]; then log "Dataset size  : $DATASET_SIZE"; fi
if [[ -n "$TOTAL_SIZE" ]]; then log "Total size    : $TOTAL_SIZE"; fi
if [[ -n "$DATA_SIZE" ]]; then log "Data size     : $DATA_SIZE"; fi
if [[ -n "$TEXT_INDEX_SIZE" ]]; then log "Text idx size : $TEXT_INDEX_SIZE"; fi
log "Parallel repl : $PARALLEL_REPLICAS"
if [[ "$PARALLEL_REPLICAS" == "1" ]]; then log "Cluster size  : ${CLUSTER_SIZE:-1}"; fi
if [[ -n "$COMMENT" ]]; then log "Comment       : $COMMENT"; fi
if [[ -n "$TAGS" ]]; then log "Tags          : $TAGS"; fi
if [[ -n "$LOAD_TIME" ]]; then log "Load time     : $LOAD_TIME"; fi
if [[ -n "$PROPRIETARY" ]]; then log "Proprietary   : $PROPRIETARY"; fi
if [[ -n "$TUNED" ]]; then log "Tuned         : $TUNED"; fi

mapfile -d '' -t QUERIES < <(extract_queries "$QUERY_FILE")
[[ "${#QUERIES[@]}" -gt 0 ]] || die "No queries found in $QUERY_FILE"

log "Loaded ${#QUERIES[@]} queries."

VERSION="$(get_version)"
DATE_STR="$(get_date_str)"

log "Resolved version : $VERSION"
log "Resolved date    : $DATE_STR"

RESULT_ROWS=()

for idx in "${!QUERIES[@]}"; do
    query_no=$((idx + 1))
    query="$(trim "${QUERIES[$idx]}")
SETTINGS enable_full_text_index=1,
         enable_parallel_replicas=${PARALLEL_REPLICAS},
         max_parallel_replicas=${CLUSTER_SIZE}"

    log "============================================================"
    log "Processing query $query_no/${#QUERIES[@]}"
    log "============================================================"

    print_query_block "$query_no" "${#QUERIES[@]}" "$query"

    restart_and_drop_caches

    runtimes=()
    for ((run=1; run<=RUNS_PER_QUERY; run++)); do
        log "Starting run $run/$RUNS_PER_QUERY for query $query_no..."
        runtime="$(run_query "$query" "$run")"
        runtimes+=("$runtime")
    done

    row="    [$(printf '%s' "${runtimes[0]}"; for ((i=1; i<${#runtimes[@]}; i++)); do printf ',%s' "${runtimes[$i]}"; done)]"
    RESULT_ROWS+=("$row")

    log "Finished query $query_no. Collected runtimes: [$(printf '%s' "${runtimes[0]}"; for ((i=1; i<${#runtimes[@]}; i++)); do printf ',%s' "${runtimes[$i]}"; done)]"
done

# Build result block (drop trailing comma on last row)
RESULT_CLEAN="$(
    for i in "${!RESULT_ROWS[@]}"; do
        if [[ "$i" -lt $((${#RESULT_ROWS[@]} - 1)) ]]; then
            echo "${RESULT_ROWS[$i]},"
        else
            echo "${RESULT_ROWS[$i]}"
        fi
    done
)"

# Optional fields
EXTRA_FIELDS=""
[[ -n "$DATASET_SIZE"    ]] && EXTRA_FIELDS+="  \"dataset_size\": $DATASET_SIZE,"$'\n'
[[ -n "$TOTAL_SIZE"      ]] && EXTRA_FIELDS+="  \"total_size\": $TOTAL_SIZE,"$'\n'
[[ -n "$TEXT_INDEX_SIZE" ]] && EXTRA_FIELDS+="  \"text_index_size\": $TEXT_INDEX_SIZE,"$'\n'

log "Printing result JSON to stdout..."
cat <<JSON
{
  "system": "$SYSTEM",
  "version": "$VERSION",
  "os": "$OS_NAME",
  "date": "$DATE_STR",
  "machine": "$MACHINE",
  "cluster_size": $CLUSTER_SIZE,
  "enable_parallel_replicas": $PARALLEL_REPLICAS,
  "proprietary": "$PROPRIETARY",
  "tuned": "$TUNED",
  "comment": "$COMMENT",
  "tags": $TAGS,
  "load_time": $LOAD_TIME,
  "data_size": $DATA_SIZE,
${EXTRA_FIELDS}  "result": [
$RESULT_CLEAN
  ]
}
JSON

log "Benchmark script finished successfully."
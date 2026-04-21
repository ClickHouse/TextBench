#!/usr/bin/env bash

set -euo pipefail

# Check if the required arguments are provided
if [[ $# -lt 6 || $# -gt 7 ]]; then
    echo "Usage: $0 <DB_NAME> <TABLE_NAME> <DATA_DIRECTORY> <NUM_FILES> <SUCCESS_LOG> <ERROR_LOG> [PARALLEL_WORKERS]"
    echo
    echo "Remote ClickHouse Cloud connection (optional env vars):"
    echo "  export FQDN=<host>        e.g. wjgkgcnnmt.us-east-2.aws.clickhouse-staging.com"
    echo "  export PASSWORD=<password>"
    echo "  export CH_USER=<user>     (default: default)"
    exit 1
fi

# Arguments
DB_NAME="$1"
TABLE_NAME="$2"
DATA_DIRECTORY="$3"
NUM_FILES="$4"
SUCCESS_LOG="$5"
ERROR_LOG="$6"
PARALLEL_WORKERS="${7:-1}"

# Validate arguments
[[ ! -d "$DATA_DIRECTORY" ]] && { echo "Error: Data directory '$DATA_DIRECTORY' does not exist."; exit 1; }
[[ ! "$NUM_FILES" =~ ^[0-9]+$ ]] && { echo "Error: NUM_FILES must be a positive integer."; exit 1; }
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
    echo "Connecting to remote ClickHouse Cloud: ${CH_USER}@${FQDN}"
else
    echo "Connecting to local ClickHouse (set FQDN and PASSWORD to target a remote service)"
fi

echo "Creating database $DB_NAME"
clickhouse client "${CH_OPTS[@]}" --query "CREATE DATABASE IF NOT EXISTS $DB_NAME"

echo "Executing DDL for database $DB_NAME"
clickhouse client "${CH_OPTS[@]}" --database="$DB_NAME" --multiquery < create.sql

echo "Loading data for database $DB_NAME with $PARALLEL_WORKERS worker(s)"
./load_data.sh "$DATA_DIRECTORY" "$DB_NAME" "$TABLE_NAME" "$NUM_FILES" "$SUCCESS_LOG" "$ERROR_LOG" "$PARALLEL_WORKERS"
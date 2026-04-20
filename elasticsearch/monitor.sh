#!/bin/bash
# Captures ES ingest and system metrics every INTERVAL seconds.
# Output: one JSON object per line (ndjson) → easy to parse later.
#
# Usage: ./monitor.sh [interval_seconds] [output_file]
#   ./monitor.sh 60 /tmp/metrics.ndjson

INTERVAL="${1:-60}"
OUTPUT="${2:-/tmp/metrics.ndjson}"
ES_URL="${ES_URL:-http://localhost:9200}"

echo "Monitoring every ${INTERVAL}s → $OUTPUT  (Ctrl-C to stop)"

# Snapshot the previous indexing total so we can compute docs/s
PREV_TOTAL=0
PREV_TIME_MS=0
PREV_TS=0

while true; do
    TS=$(date -u +%s)
    DATETIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    # ---- ES node stats ----
    NODE_STATS=$(curl -sf "$ES_URL/_nodes/stats/indices,os,jvm,thread_pool" 2>/dev/null) || NODE_STATS="{}"

    # ---- ES cat indices (all otel_logs_part_*) ----
    CAT=$(curl -sf "$ES_URL/_cat/indices/otel_logs_part_*?h=index,docs.count,store.size,pri.store.size&bytes=b&format=json" 2>/dev/null) || CAT="[]"

    # ---- System: CPU, memory, disk ----
    CPU_IDLE=$(vmstat 1 2 | tail -1 | awk '{print $15}')
    CPU_IOWAIT=$(vmstat 1 2 | tail -1 | awk '{print $16}')
    MEM_FREE_MB=$(free -m | awk '/^Mem:/{print $4}')
    MEM_AVAILABLE_MB=$(free -m | awk '/^Mem:/{print $7}')
    DISK_UTIL=$(iostat -x /dev/nvme1n1 1 2 2>/dev/null | awk '/nvme1n1/{last=$NF} END{print last}')
    DISK_READ_KB=$(iostat -x /dev/nvme1n1 1 2 2>/dev/null | awk '/nvme1n1/{last=$6} END{print last}')
    DISK_WRITE_KB=$(iostat -x /dev/nvme1n1 1 2 2>/dev/null | awk '/nvme1n1/{last=$7} END{print last}')

    # ---- Derive docs/s from indexing delta ----
    IDX_TOTAL=$(echo "$NODE_STATS" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    node = list(d['nodes'].values())[0]
    print(node['indices']['indexing']['index_total'])
except:
    print(0)
" 2>/dev/null)

    if [[ $PREV_TS -gt 0 && $IDX_TOTAL -gt $PREV_TOTAL ]]; then
        ELAPSED=$((TS - PREV_TS))
        DOCS_DELTA=$((IDX_TOTAL - PREV_TOTAL))
        DOCS_PER_SEC=$(echo "scale=1; $DOCS_DELTA / $ELAPSED" | bc)
    else
        DOCS_PER_SEC=0
    fi
    PREV_TOTAL=$IDX_TOTAL
    PREV_TS=$TS

    # ---- Emit JSON record ----
    export NODE_STATS CAT DATETIME DOCS_PER_SEC CPU_IDLE CPU_IOWAIT \
           MEM_FREE_MB MEM_AVAILABLE_MB DISK_UTIL DISK_READ_KB DISK_WRITE_KB
    python3 -c "
import json, os

node_stats = json.loads(os.environ['NODE_STATS']) if os.environ.get('NODE_STATS') else {}
cat        = json.loads(os.environ['CAT'])        if os.environ.get('CAT')        else []
nodes = node_stats.get('nodes', {})
node  = list(nodes.values())[0] if nodes else {}

def get(d, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict): return default
        d = d.get(k, default)
    return d

record = {
    'ts':           os.environ['DATETIME'],
    'docs_per_sec': float(os.environ.get('DOCS_PER_SEC', 0)),
    'indexing': {
        'total':    get(node, 'indices', 'indexing', 'index_total',          default=0),
        'current':  get(node, 'indices', 'indexing', 'index_current',        default=0),
        'time_ms':  get(node, 'indices', 'indexing', 'index_time_in_millis', default=0),
    },
    'merges': {
        'current':     get(node, 'indices', 'merges', 'current',                            default=0),
        'total':       get(node, 'indices', 'merges', 'total',                              default=0),
        'throttle_ms': get(node, 'indices', 'merges', 'total_throttled_time_in_millis',     default=0),
    },
    'bulk_thread_pool': {
        'active':    get(node, 'thread_pool', 'write', 'active',    default=0),
        'queue':     get(node, 'thread_pool', 'write', 'queue',     default=0),
        'rejected':  get(node, 'thread_pool', 'write', 'rejected',  default=0),
        'completed': get(node, 'thread_pool', 'write', 'completed', default=0),
    },
    'jvm': {
        'heap_used_pct': get(node, 'jvm', 'mem', 'heap_used_percent',                                 default=0),
        'heap_used_mb':  get(node, 'jvm', 'mem', 'heap_used_in_bytes',                                default=0) // 1048576,
        'heap_max_mb':   get(node, 'jvm', 'mem', 'heap_max_in_bytes',                                 default=0) // 1048576,
        'gc_old_count':  get(node, 'jvm', 'gc',  'collectors', 'old', 'collection_count',             default=0),
        'gc_old_ms':     get(node, 'jvm', 'gc',  'collectors', 'old', 'collection_time_in_millis',    default=0),
    },
    'os': {
        'cpu_percent':      get(node, 'os', 'cpu', 'percent', default=0),
        'cpu_idle_pct':     int(os.environ.get('CPU_IDLE',     0) or 0),
        'cpu_iowait_pct':   int(os.environ.get('CPU_IOWAIT',   0) or 0),
        'mem_free_mb':      int(os.environ.get('MEM_FREE_MB',  0) or 0),
        'mem_available_mb': int(os.environ.get('MEM_AVAILABLE_MB', 0) or 0),
    },
    'disk': {
        'util_pct':   float(os.environ.get('DISK_UTIL',     0) or 0),
        'read_kb_s':  float(os.environ.get('DISK_READ_KB',  0) or 0),
        'write_kb_s': float(os.environ.get('DISK_WRITE_KB', 0) or 0),
    },
    'indices': cat,
}
print(json.dumps(record))
" >> "$OUTPUT"
    unset NODE_STATS CAT

    # Print a brief summary to stdout
    SUMMARY=$(python3 -c "
import json, os
node_stats = json.loads(os.environ['NODE_STATS']) if os.environ.get('NODE_STATS') else {}
node = list(node_stats.get('nodes', {}).values())[0] if node_stats.get('nodes') else {}
queue = node.get('thread_pool', {}).get('write', {}).get('queue', '?')
heap  = node.get('jvm', {}).get('mem', {}).get('heap_used_percent', '?')
print(f\"bulk_queue={queue}  heap={heap}%\")
" 2>/dev/null)
    echo "$DATETIME  docs/s=${DOCS_PER_SEC}  ${SUMMARY}  disk_util=${DISK_UTIL}%  iowait=${CPU_IOWAIT}%"

    sleep "$INTERVAL"
done

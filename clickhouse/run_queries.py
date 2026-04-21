#!/usr/bin/env python3
"""
run_queries.py - ClickHouse benchmark script using the HTTP interface.

Usage:
  ./run_queries.py --query-file queries.sql --database text_bench_50B \
    --machine "236GiB" --parallel-replicas 1 --cluster-size 3 \
    --dataset-size 50000000000 --data-size 99068986216 \
    --total-size 99560268152 --text-index-size 491281201 \
    > results/50B.aws.3.236.parallel_replicas.json

Remote ClickHouse Cloud (env vars):
  export FQDN=wjgkgcnnmt.us-east-2.aws.clickhouse-staging.com
  export PASSWORD=...
  export CH_USER=default   # optional, default: default
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _ts():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def log(msg):   print(f'[{_ts()}] [INFO]  {msg}', file=sys.stderr)
def warn(msg):  print(f'[{_ts()}] [WARN]  {msg}', file=sys.stderr)
def error(msg): print(f'[{_ts()}] [ERROR] {msg}', file=sys.stderr)

def die(msg):
    error(msg)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def ch_url():
    fqdn = os.environ.get('FQDN', '')
    password = os.environ.get('PASSWORD', '')
    if fqdn and password:
        return f'https://{fqdn}:8443'
    return 'http://localhost:8123'

def ch_headers():
    fqdn = os.environ.get('FQDN', '')
    password = os.environ.get('PASSWORD', '')
    user = os.environ.get('CH_USER', 'default')
    if fqdn and password:
        return {
            'X-ClickHouse-User': user,
            'X-ClickHouse-Key': password,
        }
    return {}

def is_remote():
    return bool(os.environ.get('FQDN') and os.environ.get('PASSWORD'))


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def run_query(query, database, dry_run=False):
    """Execute query via HTTP. Returns (elapsed_seconds, result_text)."""
    if dry_run:
        return 0.0, ''

    url = f'{ch_url()}/?{urllib.parse.urlencode({"database": database})}'
    headers = ch_headers()
    headers['Content-Type'] = 'text/plain; charset=utf-8'

    req = urllib.request.Request(url, data=query.encode('utf-8'), headers=headers)

    try:
        with urllib.request.urlopen(req) as resp:
            summary = json.loads(resp.headers.get('X-ClickHouse-Summary', '{}'))
            result = resp.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        raise RuntimeError(f'HTTP {e.code}: {body}')

    elapsed = int(summary.get('elapsed_ns', 0)) / 1e9

    return elapsed, result


# ---------------------------------------------------------------------------
# Query file parsing
# ---------------------------------------------------------------------------

def extract_queries(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    queries = []
    for chunk in content.split(';'):
        lines = [l for l in chunk.split('\n') if not l.strip().startswith('--')]
        q = '\n'.join(lines).strip()
        if q:
            queries.append(q)
    return queries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_version(database, dry_run):
    if dry_run:
        return 'dry-run'
    try:
        _, result = run_query('SELECT version()', database)
        return result.strip()
    except Exception as e:
        warn(f'Could not fetch version: {e}')
        return 'unknown'

def get_os_name():
    try:
        with open('/etc/os-release') as f:
            for line in f:
                if line.startswith('PRETTY_NAME='):
                    return line.split('=', 1)[1].strip().strip('"')
    except Exception:
        pass
    return platform.platform()

def restart_and_drop_caches(database, dry_run):
    if is_remote():
        log('Remote mode: skipping restart_and_drop_caches.')
        return

    log('Stopping ClickHouse...')
    subprocess.run(['sudo', 'clickhouse', 'stop'], check=True)
    log('ClickHouse stopped.')

    log('Dropping Linux page cache...')
    subprocess.run('echo 3 | sudo tee /proc/sys/vm/drop_caches', shell=True, check=True)
    log('Linux page cache dropped.')

    log('Starting ClickHouse...')
    subprocess.run(['sudo', 'clickhouse', 'start'], check=True)
    log('ClickHouse start command issued.')

    log('Waiting for ClickHouse to become ready...')
    for attempt in range(1, 61):
        try:
            run_query('SELECT 1', database, dry_run=dry_run)
            log('ClickHouse is ready.')
            return
        except Exception:
            log(f'Not ready yet (attempt {attempt}/60), sleeping 1s...')
            time.sleep(1)
    die('ClickHouse did not become ready within 60 seconds.')


def print_query_block(query_no, total, query):
    sep = '=' * 60
    print(sep, file=sys.stderr)
    print(f'QUERY {query_no}/{total}', file=sys.stderr)
    print(sep, file=sys.stderr)
    print(query, file=sys.stderr)
    print(sep, file=sys.stderr)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description='ClickHouse HTTP benchmark runner')
    p.add_argument('--query-file',        required=True)
    p.add_argument('--database',          required=True)
    p.add_argument('--dry-run',           action='store_true')
    p.add_argument('--runs',              type=int,   default=3)
    p.add_argument('--machine',           default='')
    p.add_argument('--dataset-size',      type=int,   default=None)
    p.add_argument('--total-size',        type=int,   default=None)
    p.add_argument('--data-size',         type=int,   default=0)
    p.add_argument('--text-index-size',   type=int,   default=None)
    p.add_argument('--cluster-size',      type=int,   default=1)
    p.add_argument('--parallel-replicas', type=int,   default=0, choices=[0, 1])
    p.add_argument('--comment',           default='')
    p.add_argument('--tags',              default='["C++","column-oriented","ClickHouse","managed","aws"]')
    p.add_argument('--load-time',         type=int,   default=0)
    p.add_argument('--proprietary',       default='yes')
    p.add_argument('--tuned',             default='no')
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    comment = (
        f'{args.comment} (enable_parallel_replicas={args.parallel_replicas})'
        if args.comment else
        f'(enable_parallel_replicas={args.parallel_replicas})'
    )

    log('Benchmark script started.')
    log(f'Query file    : {args.query_file}')
    log(f'Database      : {args.database}')
    log(f'Dry run       : {args.dry_run}')
    log(f'Runs/query    : {args.runs}')
    if is_remote():
        user = os.environ.get('CH_USER', 'default')
        log(f'Target        : {user}@{os.environ["FQDN"]} (ClickHouse Cloud / HTTP)')
    else:
        log('Target        : localhost:8123 (HTTP)')
    log(f'Parallel repl : {args.parallel_replicas}')
    if args.parallel_replicas:
        log(f'Cluster size  : {args.cluster_size}')

    queries = extract_queries(args.query_file)
    if not queries:
        die(f'No queries found in {args.query_file}')
    log(f'Loaded {len(queries)} queries.')

    version  = get_version(args.database, args.dry_run)
    date_str = date.today().isoformat()
    os_name  = get_os_name()

    log(f'Resolved version : {version}')
    log(f'Resolved date    : {date_str}')

    result_rows = []

    for idx, query in enumerate(queries):
        query_no = idx + 1

        full_query = (
            f'{query}\n'
            f'SETTINGS enable_full_text_index=1,\n'
            f'         enable_parallel_replicas={args.parallel_replicas},\n'
            f'         max_parallel_replicas={args.cluster_size}'
        )

        log('=' * 60)
        log(f'Processing query {query_no}/{len(queries)}')
        log('=' * 60)
        print_query_block(query_no, len(queries), full_query)

        restart_and_drop_caches(args.database, args.dry_run)

        runtimes = []
        for run in range(1, args.runs + 1):
            log(f'Starting run {run}/{args.runs} for query {query_no}...')

            if args.dry_run:
                elapsed, result_text = 0.0, ''
            else:
                try:
                    elapsed, result_text = run_query(full_query, args.database)
                except RuntimeError as e:
                    error(f'Query run {run} failed.')
                    error(f'Query was:\n{full_query}')
                    error(str(e))
                    sys.exit(1)

            print(f'-------------------- RESULT (run {run}) --------------------', file=sys.stderr)
            print(result_text.strip() if result_text.strip() else '[no rows returned]', file=sys.stderr)
            print('-------------------------------------------------------------', file=sys.stderr)

            log(f'Run {run} completed in {elapsed:.3f}s.')
            runtimes.append(round(elapsed, 3))

        result_rows.append(runtimes)
        log(f'Finished query {query_no}. Runtimes: {runtimes}')

    # Build output JSON
    out = {
        'system':                   'ClickHouse',
        'version':                  version,
        'os':                       os_name,
        'date':                     date_str,
        'machine':                  args.machine,
        'cluster_size':             args.cluster_size,
        'enable_parallel_replicas': args.parallel_replicas,
        'proprietary':              args.proprietary,
        'tuned':                    args.tuned,
        'comment':                  comment,
        'tags':                     json.loads(args.tags),
        'load_time':                args.load_time,
        'data_size':                args.data_size,
    }

    if args.dataset_size   is not None: out['dataset_size']    = args.dataset_size
    if args.total_size     is not None: out['total_size']       = args.total_size
    if args.text_index_size is not None: out['text_index_size'] = args.text_index_size

    out['result'] = result_rows

    log('Printing result JSON to stdout...')
    print(json.dumps(out, indent=2))
    log('Benchmark script finished successfully.')


if __name__ == '__main__':
    main()
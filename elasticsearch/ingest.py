#!/usr/bin/env python3
"""
High-throughput Parquet → Elasticsearch ingest.

Splits Parquet row groups across N processes (bypasses GIL) so that
type conversion and JSON serialization run truly in parallel.
Each process also runs M bulk-request threads to saturate ES.

When multiple indices are given (comma-separated), each batch is fanned out
to all indices in parallel threads — the expensive Parquet→JSON conversion
happens only once.

Total concurrency: N processes × M bulk threads × K indices concurrent posts.

Usage:
    python3 ingest.py --index otel_logs --files 1
    python3 ingest.py --index otel_logs,otel_logs_source,otel_logs_synthetic --files 10
"""

import argparse
import os
import queue
import random
import threading
import time
from datetime import datetime, timezone
from multiprocessing import Pool

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")


# ---------------------------------------------------------------------------
# Type conversion
# ---------------------------------------------------------------------------

def _format_ts_ns(ns: int) -> str:
    secs = ns // 1_000_000_000
    nanos = ns % 1_000_000_000
    dt = datetime.fromtimestamp(secs, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def _format_ts_us(us: int) -> str:
    secs = us // 1_000_000
    ms = (us % 1_000_000) // 1_000
    dt = datetime.fromtimestamp(secs, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"


def batch_to_ndjson(batch: pa.RecordBatch) -> bytes:
    """Convert a RecordBatch to NDJSON bytes (action + doc per pair)."""
    cols = {}
    for col_name in batch.schema.names:
        col = batch.column(col_name)
        t = col.type
        if pa.types.is_timestamp(t):
            int_vals = col.cast(pa.int64()).to_pylist()
            if t.unit == "ns":
                cols[col_name] = [_format_ts_ns(v) if v is not None else None for v in int_vals]
            elif t.unit == "us":
                cols[col_name] = [_format_ts_us(v) if v is not None else None for v in int_vals]
            else:
                cols[col_name] = [v.isoformat() if v is not None else None for v in col.to_pylist()]
        elif pa.types.is_map(t):
            cols[col_name] = [dict(v) if v is not None else None for v in col.to_pylist()]
        else:
            cols[col_name] = col.to_pylist()

    names = list(cols.keys())
    values = list(cols.values())
    action = b'{"index":{}}\n'
    parts = []
    for row in zip(*values):
        parts.append(action)
        parts.append(orjson.dumps(dict(zip(names, row)), option=orjson.OPT_NON_STR_KEYS) + b"\n")
    return b"".join(parts)


# ---------------------------------------------------------------------------
# Elasticsearch
# ---------------------------------------------------------------------------

def bulk_post(session: requests.Session, index: str, body: bytes, n_docs: int) -> tuple[int, int]:
    backoff = 1.0
    retries = 0
    while True:
        try:
            resp = session.post(
                f"{ES_URL}/{index}/_bulk",
                data=body,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=120,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            retries += 1
            sleep_s = backoff + random.uniform(0, backoff)
            print(f"  Connection error ({e.__class__.__name__}) — retry {retries}, backing off {sleep_s:.1f}s...", flush=True)
            time.sleep(sleep_s)
            backoff = min(backoff * 2, 60.0)
            # Recreate session after connection failure
            session = requests.Session()
            continue
        if resp.status_code == 429:
            retries += 1
            sleep_s = backoff + random.uniform(0, backoff)
            print(f"  429 — backing off {sleep_s:.1f}s...", flush=True)
            time.sleep(sleep_s)
            backoff = min(backoff * 2, 60.0)
            continue
        resp.raise_for_status()
        result = resp.json()
        errors = sum(1 for item in result.get("items", []) if item.get("index", {}).get("error"))
        if errors:
            print(f"  WARNING: {errors}/{n_docs} indexing errors in {index}", flush=True)
        return n_docs - errors, retries


# ---------------------------------------------------------------------------
# Worker: one process handles a slice of row groups
# ---------------------------------------------------------------------------

def ingest_segment(args: tuple) -> dict:
    """Multiprocessing worker — reads row groups [rg_start, rg_end) from disk."""
    file_path, indices, rg_start, rg_end, batch_size, bulk_workers = args
    tag = f"[rg {rg_start:04d}-{rg_end:04d}]"

    pf = pq.ParquetFile(file_path)

    ndjson_q: queue.Queue = queue.Queue(maxsize=bulk_workers * 4)
    stats = {"indexed": 0, "retries": 0}
    lock = threading.Lock()

    def bulk_worker():
        # One persistent session per index, reused across all batches in this worker.
        sessions = {idx: requests.Session() for idx in indices}
        while True:
            item = ndjson_q.get()
            if item is None:
                ndjson_q.task_done()
                break
            body, n_docs = item

            if len(indices) == 1:
                # Fast path: no fan-out overhead
                ok, retries = bulk_post(sessions[indices[0]], indices[0], body, n_docs)
            else:
                # Fan-out: post to all indices concurrently, NDJSON prepared only once
                fan_results: dict[str, tuple[int, int]] = {}

                def post_to(idx: str) -> None:
                    fan_results[idx] = bulk_post(sessions[idx], idx, body, n_docs)

                fan_threads = [threading.Thread(target=post_to, args=(idx,)) for idx in indices]
                for t in fan_threads:
                    t.start()
                for t in fan_threads:
                    t.join()

                # Count docs indexed into the primary index; sum retries across all
                ok = fan_results[indices[0]][0]
                retries = sum(r[1] for r in fan_results.values())

            with lock:
                stats["indexed"] += ok
                stats["retries"] += retries
            ndjson_q.task_done()

    workers = [threading.Thread(target=bulk_worker, daemon=True) for _ in range(bulk_workers)]
    for w in workers:
        w.start()

    t0 = time.monotonic()
    last_log = t0
    docs_at_last_log = 0
    total_rgs = rg_end - rg_start

    for rg_idx in range(rg_start, rg_end):
        for batch in pf.read_row_group(rg_idx).to_batches(max_chunksize=batch_size):
            ndjson_q.put((batch_to_ndjson(batch), batch.num_rows))

        now = time.monotonic()
        if now - last_log >= 30:
            with lock:
                current = stats["indexed"]
            interval_rate = (current - docs_at_last_log) / (now - last_log)
            rgs_done = rg_idx - rg_start + 1
            print(
                f"{tag} {rgs_done}/{total_rgs} row groups  "
                f"{current:,} docs  {interval_rate:,.0f} docs/s",
                flush=True,
            )
            last_log = now
            docs_at_last_log = current

    for _ in workers:
        ndjson_q.put(None)
    for w in workers:
        w.join()

    elapsed = time.monotonic() - t0
    rate = stats["indexed"] / elapsed if elapsed > 0 else 0
    print(f"{tag} done — {stats['indexed']:,} docs  {rate:,.0f} docs/s avg", flush=True)
    return stats


# ---------------------------------------------------------------------------
# Per-file orchestration
# ---------------------------------------------------------------------------

def process_file(file_path: str, indices: list[str], batch_size: int, num_processes: int, bulk_workers: int) -> int:
    pf = pq.ParquetFile(file_path)
    total_rows = pf.metadata.num_rows
    total_rg = pf.metadata.num_row_groups
    print(
        f"{file_path}: {total_rows:,} rows / {total_rg} row groups → "
        f"{num_processes} processes × {bulk_workers} bulk workers × {len(indices)} indices",
        flush=True,
    )

    rg_per_proc = (total_rg + num_processes - 1) // num_processes
    segments = [
        (file_path, indices, i * rg_per_proc, min((i + 1) * rg_per_proc, total_rg), batch_size, bulk_workers)
        for i in range(num_processes)
        if i * rg_per_proc < total_rg
    ]

    t0 = time.monotonic()
    with Pool(processes=len(segments)) as pool:
        results = pool.map(ingest_segment, segments)

    elapsed = time.monotonic() - t0
    total = sum(r["indexed"] for r in results)
    print(f"File done: {total:,} docs in {elapsed:.1f}s  ({total/elapsed:,.0f} docs/s)", flush=True)
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", required=True,
                        help="Index name(s), comma-separated (e.g. otel_logs,otel_logs_source)")
    parser.add_argument("--files",        type=int, required=True,
                        help="Number of files to ingest (1=1B rows, 10=10B, 50=50B)")
    parser.add_argument("--start-file",   type=int, default=0)
    parser.add_argument("--processes",    type=int, default=8,
                        help="Parallel processes per file (default: 8)")
    parser.add_argument("--bulk-workers", type=int, default=4,
                        help="Bulk HTTP threads per process (default: 4)")
    parser.add_argument("--batch-size",   type=int, default=50000)
    parser.add_argument("--local-dir",    default="/tmp")
    args = parser.parse_args()

    indices = [idx.strip() for idx in args.index.split(",")]

    t0 = time.monotonic()
    grand_total = 0
    for fn in range(args.start_file, args.start_file + args.files):
        path = os.path.join(args.local_dir, f"part_{fn:03d}.parquet")
        grand_total += process_file(path, indices, args.batch_size, args.processes, args.bulk_workers)

    elapsed = time.monotonic() - t0
    print(f"\nGrand total: {grand_total:,} docs in {elapsed:.1f}s  ({grand_total/elapsed:,.0f} docs/s)")


if __name__ == "__main__":
    main()

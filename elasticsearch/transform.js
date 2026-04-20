/**
 * Transform applied during ingest to fix two Arrow → Elasticsearch mismatches:
 *
 * 1. Nanosecond timestamps (Arrow timestamp[ns]) arrive as BigInt.
 *    ES `date_nanos` accepts ISO 8601 with 9 decimal places — JavaScript's
 *    Date loses sub-millisecond precision, so we format manually.
 *
 * 2. Map<String,String> columns (ResourceAttributes, ScopeAttributes,
 *    LogAttributes) arrive as JS Map objects from Arrow.
 *    ES `flattened` expects a plain JSON object.
 */

const MAP_FIELDS = new Set(["ResourceAttributes", "ScopeAttributes", "LogAttributes"]);

/** Format BigInt nanoseconds-since-epoch → "2025-09-23T00:00:00.123456789Z" */
function formatNs(ns) {
  const secs = ns / 1_000_000_000n;
  const nanos = Number(ns % 1_000_000_000n);
  const dt = new Date(Number(secs) * 1000);
  const base = dt.toISOString().replace(/\.\d{3}Z$/, "");
  return `${base}.${String(nanos).padStart(9, "0")}Z`;
}

/** Convert any timestamp value to an ISO string safe for ES date / date_nanos. */
function toIso(v, nanosecondPrecision = false) {
  if (v == null) return null;
  if (typeof v === "bigint") {
    return nanosecondPrecision ? formatNs(v) : new Date(Number(v / 1_000_000n)).toISOString();
  }
  if (v instanceof Date) return v.toISOString();
  if (typeof v === "number") return new Date(v).toISOString();
  return v; // already a string
}

/** Convert an Arrow Map / JS Map / array-of-pairs to a plain object. */
function toPlainObject(v) {
  if (v == null) return null;
  if (v instanceof Map) return Object.fromEntries(v);
  if (Array.isArray(v)) {
    // Arrow Map rows sometimes surface as [{key, value}, ...]
    const obj = {};
    for (const entry of v) {
      if (entry && "key" in entry) obj[entry.key] = entry.value;
    }
    return obj;
  }
  return v; // already a plain object
}

export default function transform(doc) {
  const out = { ...doc };

  // Nanosecond timestamp → date_nanos
  if (out.Timestamp !== undefined) out.Timestamp = toIso(out.Timestamp, true);

  // Second/ms timestamp → date
  if (out.TimestampTime !== undefined) out.TimestampTime = toIso(out.TimestampTime, false);

  // Map columns → plain objects for ES `flattened`
  for (const field of MAP_FIELDS) {
    if (out[field] !== undefined) out[field] = toPlainObject(out[field]);
  }

  return out;
}

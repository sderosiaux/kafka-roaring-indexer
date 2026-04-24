# kafka-roaring-indexer — SPEC

A Kafka-consumer-side pre-aggregation indexer. Reads records from a topic, extracts declared dimensions, maintains Roaring bitmap indexes and UltraLogLog sketches grouped in time-bucketed segments. Exposes an HTTP query API for filters + distinct-count / cardinality aggregations.

No broker fork. No Kafka protocol changes. Pure consumer-side sidecar, configurable via a single YAML.

---

## 1. Goals / non-goals

### Goals

- Declarative, reproducible indexing from Kafka (one YAML → one indexer process).
- Exact filtering (AND/OR/NOT on dimensions) via Roaring bitmaps.
- Approximate distinct-count (~0.8% error) via UltraLogLog sketches.
- Time-bucketed immutable segments on disk, queryable via HTTP.
- Replay-safe (restart, crash, partition rebalance) through idempotent ops + Kafka offset commit ordered after index fsync.
- POC-scale: single process, single topic, 10k–100k msg/s.

### Non-goals (for v1)

- Distributed query / sharding across multiple indexer instances.
- Full SQL. Query is structured filter + single aggregation.
- Exact count-distinct on high-cardinality fields without an explicit dict.
- Historical backfill beyond `startOffset=earliest`.
- Writes or mutation of Kafka topics.
- Stream joins, windowed aggregations beyond time-bucket segments.

---

## 2. Data model

### 2.1 Dimensions

A dimension is a field extracted from each record, with a chosen encoding that maps values to `uint32` for bitmap membership.

```
dimension(name, field, encoding, maxCardinality)
  encoding ∈ {dict, raw_uint32, hash32, hash64}
```

Per-segment storage per dimension:

```
Map<value_id:uint32, RoaringBitmap<member_id:uint32>>
```

Where `member_id` is the entity whose set-membership we track (typically `userId`).

### 2.2 Member

Defined globally (one per indexer). All bitmaps store members of this type.

```
member(field, encoding)
  encoding ∈ {raw_uint32, dict, hash64}
```

Choice drives what distinct-count means:

- `member = userId` → `bitmap.cardinality()` = distinct users matching a filter.
- `member = offset` → bitmap stores record offsets → drill-down to records, not distinct users.

For the POC: `member = userId`.

### 2.3 Metrics (sketches)

Non-set aggregations stored per segment:

```
metric(name, type, field, precision, sliceBy)
  type ∈ {ull, hll, cpc, count, sum}
```

- `ull`: UltraLogLog (Ertl 2023). Default distinct-count sketch. ~0.8% error at `precision=12` (4 KB).
- `hll`: classic HyperLogLog (legacy / compat).
- `cpc`: Compressed Probabilistic Counting (Apache DataSketches). Smaller on disk, slower merge.
- `count`, `sum`: exact scalar accumulators.

`sliceBy` produces one sketch per value tuple of the slicing dimensions. Cartesian product — guard with low-cardinality dims only.

### 2.4 Segment

Immutable unit of storage, anchored on a time bucket.

```
segment {
  id:              monotonic uint64
  time_range:      [t_start, t_end)   // aligned to bucket boundary
  first_offset:    map<partition, uint64>
  last_offset:     map<partition, uint64>
  record_count:    uint64
  dims:            map<dim_name, map<value_id, RoaringBitmap>>
  dicts:           map<dim_name, Dict>     // for encoding=dict
  metrics:         map<metric_name, map<slice_tuple, Sketch>>
  member_dict:     Dict?                  // if member.encoding=dict (usually global ref)
  created_at:      timestamp
  schema_version:  uint32
}
```

On rollover: freeze, `runOptimize()` bitmaps, serialize (portable Roaring), fsync, commit Kafka offsets.

### 2.5 Global member dictionary (optional)

When `member.encoding=dict`, a single append-only dict `string → uint32` is maintained across the lifetime of the indexer, persisted with snapshot semantics.

Strict rules:

- Append-only, never reassign ids.
- Snapshotted to disk every `snapshotEvery`, fsync'd before Kafka commit references snapshot.
- Overflow (`maxSize` reached): reject new entries and route record to `overflow.log` or fail (configurable).

---

## 3. DSL (YAML)

Single source of truth. See `examples/events-analytics.yaml` for a full walkthrough. JSON Schema in `schema/indexer.schema.json`.

Top-level sections:

| Section          | Purpose                                                             |
| ---------------- | ------------------------------------------------------------------- |
| `metadata`       | Name, description                                                   |
| `source`         | Kafka broker, topic, consumer group, Schema Registry, parallelism   |
| `segmentation`   | Bucket size, alignment, retention                                   |
| `fields`         | Named field extractors (path + type), referenced below              |
| `timeField`      | Which field anchors records to buckets (default: Kafka timestamp)   |
| `member`         | Entity tracked by bitmaps                                           |
| `dimensions`     | Filterable indexes (Roaring)                                        |
| `metrics`        | Aggregatable sketches (ULL/HLL/CPC/count/sum)                       |
| `storage`        | On-disk layout, compression, mmap                                   |
| `query`          | HTTP API binding, limits                                            |
| `observability`  | Metrics, logs                                                       |

### 3.1 Validation at boot

Mandatory, in order:

1. Parse + schema-validate YAML.
2. Resolve Schema Registry for the topic. Match each `field.path` against the schema, verify type compatibility with declared `type`.
3. For each `dimension.field`: verify `encoding` is compatible with the field's runtime type.
4. For each `metric.field`: same.
5. Verify `storage.path` writable, enough free disk (heuristic: 1 GB min).
6. Dry-run: poll 1 record, run the extraction pipeline, log result.
7. Only then: join the consumer group and start.

Fail-fast — never start consumption with an invalid config.

---

## 4. Ingestion pipeline

```
Kafka poll
  │
  ▼
deserialize (Avro/Proto/JSON via Schema Registry)
  │
  ▼
extract fields (by path)
  │
  ▼
resolve time bucket ─── select or create segment
  │
  ▼
for each dimension: encode value → uint32
  │
  ▼
encode member → uint32 (via member_dict if needed)
  │
  ▼
for each dimension:
    segment.dims[dim_name][value_id].add(member_id)
  │
  ▼
for each metric:
    update sketch(es), sliced if configured
  │
  ▼
advance in-memory counters (record_count, last_offset)
  │
  ▼
(async) segment roll trigger?
    yes → freeze, runOptimize, serialize, fsync, commit Kafka offsets
```

Key invariant: **Kafka offsets are committed only after the segment (or incremental checkpoint) covering them is durably on disk.** On crash, at-least-once replay is safe thanks to idempotent `bitmap.add()`.

---

## 5. Query model

### 5.1 HTTP API

```
GET /query
    ?topic=<name>
    &from=<ISO-8601>
    &to=<ISO-8601>
    &filter=<expression>
    &agg=<metric_name | cardinality | count>
    [&groupBy=<time_bucket>]
```

Filter grammar (minimal):

```
expr    := term ( ('AND'|'OR') term )*
term    := '(' expr ')' | predicate | 'NOT' term
predicate := dim ':' value (',' value)*
```

Example:

```
filter = country:FR,DE AND device:mobile AND NOT status:5xx
```

`country:FR,DE` expands to `OR` inside the dim. AND/OR across dims.

### 5.2 Execution

```
1. Parse filter → AST.
2. Find segments overlapping [from, to].
3. For each segment s, reduce filter to RoaringBitmap:
     country:FR,DE → OR(s.dim("country","FR"), s.dim("country","DE"))
     device:mobile → s.dim("device","mobile")
     NOT status:5xx → flip(s.dim("status","500"), s.dim("status","503"), …)
                      within segment's member universe
     AND/OR → RoaringBitmap ops
   → matched_s
4. Aggregate:
     agg=cardinality           → sum of matched_s.cardinality()
                                 (careful: double-counting if member appears in multiple segments
                                  → MERGE bitmaps instead of summing cardinalities)
     agg=<ull_metric>          → if metric.sliceBy matches filter exactly:
                                     merge precomputed sketches → estimate
                                   else: best-effort rebuild ULL from matched bitmap
                                   (requires member == sketch.field)
     agg=count                 → sum of matched_s.cardinality() if member=offset,
                                   else explicit count metric
5. Serialize response (JSON).
```

### 5.3 Cross-segment distinct-count

Two cases:

1. **Member is the sketch field** (e.g. distinctUsers on userId, member=userId):
   `matched` bitmap across all segments → OR them → cardinality is exact.

2. **Sketch field differs from member** (e.g. distinctPaths):
   Cannot rebuild from bitmap; requires using stored sketches.
   Correct merge only if the query filter exactly matches a precomputed `sliceBy`.
   Otherwise the answer is approximate/lower-bound.

v1 restriction: reject queries where case 2 has a non-matching slice, return 400 with suggested alternatives.

---

## 6. Storage layout

```
{storage.path}/
  indexer.meta                # top-level state, schema version
  member_dict.v{N}.snap       # if member.encoding=dict
  segments/
    seg_{id}_{t_start}_{t_end}/
      meta.json               # first/last offsets, record_count, schema hash
      dims/
        country.rbi           # portable Roaring bitmaps, length-prefixed
        device.rbi
        status.rbi
      dicts/
        country.dict          # value → uint32 map (if dict encoding)
        device.dict
      metrics/
        distinctUsers.sk      # serialized ULL(s); one file per metric, multi-slice inside
        distinctPaths.sk
        requestCount.scalar
  overflow/
    {timestamp}.jsonl         # records rejected by maxCardinality or overflow
```

`.rbi` files: portable Roaring format (cross-language). Easy to inspect with standalone tools.
`.sk` files: sketch type + precision header + serialized bytes, length-prefixed per slice.

Compression: optional zstd frame around the whole segment dir at rollover, kept as `.tar.zst` for cold tiers.

---

## 7. Operational concerns

### 7.1 Mémoire

Rough budget per segment:

```
RAM ≈ sum over dims of (values_count × avg_bitmap_bytes)
      + sum over metrics of (slice_count × sketch_size)
      + dicts overhead
```

Example (1h segment, 10 low-card dims × ~100 values × 5 KB avg = 5 MB) + (5 ULL metrics × 100 slices × 4 KB = 2 MB) ≈ 7 MB per segment.
30d retention × 24 buckets/day = 720 segments. Cold ones mmap'd, not resident. Active working set: a handful.

### 7.2 Rebalances (consumer group)

POC: `parallelism: 1`. Single consumer, sticky assignment, no rebalance.

v1+:

- State is partitioned per Kafka partition.
- On rebalance, an indexer losing a partition fsyncs its segment state and uploads to shared storage (or serves it via HTTP for pull).
- Indexer gaining a partition replays from last committed offset until caught up.
- Or: accept state loss + replay from `earliest` on that partition (slow but simple).

### 7.3 Schema evolution

- `schema_version` captured per segment.
- Field removed: indexer refuses to continue without explicit migration directive. Old segments stay queryable but flagged.
- Field added: can be ignored or added to config on next restart. Old segments simply lack it (query returns empty bitmap for that dim within that time range).
- Type change: treat as incompatible. Require operator intervention.

### 7.4 Cardinality explosion guard

Every dimension has `maxCardinality`. Enforcement modes:

- `dropIfExceeded: false` (default): segment is marked invalid, indexer halts.
- `dropIfExceeded: true`: record goes to `overflow.log`, counter increments, processing continues.

Metrics expose `dim_cardinality{dim=…}` gauges so alerts can fire before the hard cap.

### 7.5 Replay safety

- Bitmap `add()` is idempotent.
- ULL `add()` is probabilistically idempotent — same input converges to same register state. Replay-safe.
- `count`/`sum` metrics are NOT replay-safe. Use dedupe or avoid.

v1 disallows `count`/`sum` at-least-once; require exactly-once config or forbid replay mid-segment.

---

## 8. Technology

- **Language**: Kotlin/JVM (Gradle). Rationale: best Kafka client, best Roaring Java port, aligned with Conduktor stack.
- **Kafka**: `org.apache.kafka:kafka-clients`.
- **Roaring**: `org.roaringbitmap:RoaringBitmap`.
- **Sketches**: `com.dynatrace.hash4j:hash4j` (UltraLogLog, by O. Ertl). Fallback: `org.apache.datasketches:datasketches-java` for CPC/HLL compat.
- **Schema Registry**: `io.confluent:kafka-avro-serializer` + `io.confluent:kafka-protobuf-serializer` + `io.confluent:kafka-json-schema-serializer`.
- **HTTP**: `io.javalin:javalin`.
- **Config**: `com.charleskorn.kaml` (Kotlin-native YAML) + custom validator hooking Schema Registry.
- **Observability**: Micrometer + Prometheus.

---

## 9. Milestones

Ordered, each a working demo state.

1. **Scaffolding** — Gradle, Kotlin, runnable main, empty config loader.
2. **Config parse + validate (offline)** — YAML → typed `IndexerConfig`, JSON Schema validation.
3. **Schema Registry probe** — validate fields/types at boot against a real topic.
4. **Consumer pipeline sketch** — poll, deser (Avro first), extract, log extracted dims.
5. **In-RAM bitmap indexing** — one segment, one dim. Cardinality query in REPL.
6. **HTTP query** — `/cardinality?dim=X&value=Y`.
7. **Multi-dim, AND/OR filter parser**.
8. **Time-bucketed segments, rollover, range query**.
9. **Persistence** — serialize segment on roll, load on boot.
10. **Offset commit ordered after fsync**.
11. **UltraLogLog metrics** via hash4j, merge semantics.
12. **Crash test** — kill -9, restart, verify cardinality unchanged.
13. **Bench** — throughput, p50/p99 query latency, RAM vs cardinalities.
14. **Multi-topic / multi-config support** — optional.
15. **Edge cases** — schema evolution, cardinality explosion, rebalance.

Between 6 and 9: demo-ready POC.

---

## 10. Open questions

- **Multi-partition state ownership**: split state across consumers by partition vs. single-consumer for POC? → single for now, document multi for v2.
- **`member` choice**: allow multiple members per indexer? → no, complicates query semantics. One indexer = one member. Run two for two perspectives.
- **Filter grammar**: stay minimal (AND/OR/NOT/IN) or add range predicates? → minimal for v1; numeric bucketed dims cover ranges approximately.
- **Compaction of global member dict**: allowed? → append-only for v1, offline rebuild tool in v2.
- **Segment tiering to S3**: OOS for v1. Hook exists via `storage.tier` for later.
- **TLS / auth**: consumer TLS yes (Kafka), HTTP query auth → `none` in POC, bearer in v1.

---

## 11. Glossary

- **Dimension**: a field declared as filterable; produces one bitmap per distinct value per segment.
- **Member**: the entity whose membership is tracked inside bitmaps (e.g. `userId`).
- **Segment**: immutable time-bucketed unit of index storage.
- **Roaring**: compressed bitmap format with array/bitset/run containers, SIMD-accelerated set ops.
- **UltraLogLog (ULL)**: probabilistic distinct-count sketch (Ertl 2023), mergeable, ~0.8% error at p=12.
- **Dict**: value → uint32 mapping for string-typed dimensions or members.

---

## 12. References

- Chambi, Lemire et al. *Better bitmap performance with Roaring bitmaps* (2014).
- Ertl, Otmar. *UltraLogLog: A Practical and More Space-Efficient Alternative to HyperLogLog* (2023).
- Leis, Kemper, Neumann. *The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases* (2013) — for eventual dict backing.
- Apache DataSketches: CPC sketch rationale.
- RoaringBitmap Java: https://github.com/RoaringBitmap/RoaringBitmap
- hash4j: https://github.com/dynatrace-oss/hash4j

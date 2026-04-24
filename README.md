# kafka-roaring-indexer

An experiment: what does the smallest useful pre-aggregated analytics index over a Kafka topic look like, built as a pure consumer sidecar, using Roaring bitmaps for exact filters and UltraLogLog sketches for approximate distinct counts?

No broker fork. No schema changes. No fleet of OLAP services. One YAML, one process, one HTTP endpoint.

![KRI Explorer — bitmap-indexed stream analytics](docs/screenshot.png)

## The problem

Kafka is a log. Analytics questions are not log questions.

> *How many distinct users in FR, on mobile, hit a 5xx in the last hour?*

To answer that with only Kafka, you scan. Every time. Or you push everything into a full OLAP engine — Druid, Pinot, ClickHouse, Snowflake — which solves the problem but drags along a distributed cluster, an ingestion pipeline, a schema catalog, a query planner, a cost center. For a handful of dashboards and alerts, that's three orders of magnitude too much machinery.

The interesting gap sits in the middle. Pick the right two data structures — bitmaps for set membership, sketches for cardinality — organise them by time bucket, and you can answer the *actual questions product teams ask* in milliseconds on billions of records, from a single-process sidecar that a team can reason about end to end.

## The bet

Two data structures do most of the work:

- **Roaring bitmaps** — one bitmap per `(dimension, value)` pair per time bucket, storing the set of members (usually user ids) that matched. Exact. AND / OR / NOT become one-to-three-nanosecond-per-word SIMD set operations. Compressed. Widely-used (Druid, Pinot, Lucene internals).
- **UltraLogLog** — an ~800-byte probabilistic sketch with ~0.8% error for distinct-count on high-cardinality fields where you cannot afford to store a bitmap per value. Mergeable across time buckets. Idempotent under replay.

One member type per indexer (the entity whose membership is tracked — typically `userId`). A dimension is a declared path into the record with a chosen encoding (`dict` / `raw_uint32` / `hash32` / `hash64`). Segments are time-bucketed, immutable once rolled, fsynced to disk before their Kafka offsets commit.

That's the whole model. Everything else is plumbing around it.

```mermaid
flowchart LR
  K[Kafka poll] --> D[deserialize<br/>Avro / JSON]
  D --> E[extract fields<br/>by dotted path]
  E --> T{resolve<br/>time bucket}
  T --> S[open segment<br/>or create]
  S --> B["per dim:<br/>bitmap(value_id).add(member_id)"]
  S --> M[per metric:<br/>ULL.add / count++ / sum+=]
  S -. rollover trigger .-> R[freeze +<br/>runOptimize +<br/>serialize + fsync]
  R --> C[commit Kafka<br/>offsets]
```

Per segment on disk, the data model is one bitmap per `(dimension, value)` pair over a uint32 member space, plus a handful of sketches and scalar counters:

```mermaid
graph TD
  Seg["Segment<br/>[t_start, t_end)"]
  Seg --> Dims[dims]
  Seg --> Sk[ullSketches]
  Seg --> CS["counters / sums"]
  Seg --> Off["offsets<br/>partition → first..last"]
  Dims --> C["country → { FR: RoaringBitmap,<br/>DE: RoaringBitmap, ... }"]
  Dims --> Dv["device → { mobile: RoaringBitmap,<br/>desktop: RoaringBitmap }"]
  Sk --> SU["distinctUsers → { (): ULL }"]
  Sk --> SP["distinctPaths → { (FR,mobile): ULL,<br/>(DE,desktop): ULL, ... }"]
```

## The use case

Think of a product analytics stream on a Kafka topic: one record per user event, maybe 10k–100k msg/s, a dozen dimensions (country, device, path, status, experiment bucket, …), retained for 30 days.

Things this indexer answers cheaply:

- **Distinct users matching a filter**, exact.
  `country:FR,DE AND device:mobile AND NOT status:5xx` → milliseconds.
- **Distinct-of-other** (e.g. distinct paths visited), approximate via ULL.
- **Counts, sums** per filter — the usual "how many requests in this time window with this shape?"
- **Multi-dim filtering with AND / OR / NOT / IN** over a time range spanning up to `maxSegmentsPerQuery` buckets.
- **Crash / restart / replay** without double-counting, because `bitmap.add()` is idempotent and offsets commit only after segment fsync.

Things it deliberately does **not** do:

- Full SQL.
- Joins. Windowed aggregations beyond the time-bucket boundary.
- Distributed queries. One process, one topic, one member type.
- Exact count-distinct on fields without an explicit dict (use a sketch or pick your encoding).

The design note lives in [`SPEC.md`](SPEC.md). The DSL is a single YAML validated against [`schema/indexer.schema.json`](schema/indexer.schema.json) — a full example in [`examples/events-analytics.yaml`](examples/events-analytics.yaml).

## What's interesting in the design

A few choices that aren't obvious:

- **Offsets commit *after* segment fsync.** The simplest invariant that gives replay-safety without needing exactly-once or transactions. A crash loses the tail in the active segment, which `bitmap.add()` idempotency lets us safely re-consume.

  ```mermaid
  sequenceDiagram
    participant C as Consumer
    participant S as Segment RAM
    participant D as Disk
    participant K as Kafka
    C->>S: add partition offset record
    Note over S: bitmap.add memberId<br/>ULL.add hash
    S->>S: rollover trigger
    S->>D: serialize + fsync rbi ull count
    D-->>S: durable
    S->>K: commitSync offsets
    Note over C,K: crash before commit:<br/>replay from last committed offset<br/>add is idempotent so safe
  ```

- **Per-dim encoder chosen at config time.** Dict when you'll filter on exact values. Raw uint32 when the field already fits. Hash32/64 when cardinality is huge and you only need equality, not ground truth (with the collision caveat spelled out in the config comment).
- **Filter grammar kept minimal.** AND / OR / NOT / IN, values expanded inside a dimension. Range predicates collapse into bucketed numeric dims (`linear` / `exponential` / `explicit`), which means range queries reduce to Roaring `OR` over pre-computed bucket bitmaps.
- **Portable Roaring on disk.** `.rbi` files are cross-language inspectable — no runtime lock-in.
- **Member dictionary is append-only, ever.** Cardinality guards per dimension, with `reject` / `overflow` / `halt` semantics. A dim blowing up its cardinality does not silently poison the segment.
- **Queries fan out, writes keep flowing.** Segment evaluation is a pure map/reduce: per-segment filter → `RoaringBitmap`, then `FastAggregation.or` across segments. Above a configurable threshold (`query.parallelThreshold`, default 8) the fan-out goes parallel on a dedicated `ForkJoinPool` sized by `query.parallelism`. Frozen segments are handed out as-is (their bitmaps are immutable); the currently-open segment is read under a read-lock that clones each touched bitmap so a concurrent `bitmap.add()` on the writer thread cannot corrupt the query's view. Single-writer, many-readers, no stop-the-world.

Deliberately skipped in v1, flagged in the task list: ART backing for the dict (prefix filters + radix compression on clustered keys), `Int2ObjectOpenHashMap` for the value-id map (quick RAM win), Z-order for multi-range numeric dims.

## Storage layout: partitions, segments, manifest

One Kafka topic produces one directory tree on disk. The consumer consumes every partition, and the indexer carves the data into **one segment per `(partition, time bucket)`** — the natural unit at which Kafka offsets are linear and at which a query plan wants to fan out.

```mermaid
flowchart LR
  subgraph K["Kafka topic events"]
    P0[partition 0]
    P1[partition 1]
    P2[partition 2]
    PN[partition N]
  end

  subgraph I["IndexerConsumer — single process"]
    L[poll loop]
    S["store.segmentFor(partition, ts)"]
  end

  subgraph D["/var/lib/kri/events/"]
    M[manifest.json<br/>catalog of every segment]
    subgraph SEG["segments/"]
      S1["seg p00000 10h..11h"]
      S2["seg p00001 10h..11h"]
      S3["seg p00000 11h..12h"]
      S4["..."]
    end
  end

  P0 --> L
  P1 --> L
  P2 --> L
  PN --> L
  L --> S
  S -. freeze + fsync + manifest upsert .-> S1
  S -.-> S2
  S -.-> S3
  SEG --- M
```

Rollover is the moment that matters: `seg.freeze()` runs `runOptimize` on every bitmap, `SegmentIO.write()` fsyncs the segment tree, `manifest.upsert()` replaces the manifest atomically via tmp-file-rename, and **only then** `commitSync` advances the Kafka offset for that partition. Crash between add and fsync = replay, and `bitmap.add(memberId)` is idempotent, so no double count.

### The grid: one segment per (partition, time bucket)

```
                       time buckets (1h each)
                    ┌─────┬─────┬─────┬─────┬─────┬─────┐
                    │ 10h │ 11h │ 12h │ 13h │ 14h │ 15h │
           ┌────────┼─────┼─────┼─────┼─────┼─────┼─────┤
           │   p0   │ s03 │ s16 │ s22 │ s34 │ s40 │ s52 │
  Kafka    │   p1   │ s09 │ s12 │ s25 │ s31 │ s45 │ s59 │
  partition│   p2   │ s06 │ s18 │ s20 │ s33 │ s41 │ s57 │
  of topic │  ...   │     │     │     │     │     │     │
           │   p9   │ s08 │ s15 │ s28 │ s37 │ s49 │ s50 │
           └────────┴─────┴─────┴─────┴─────┴─────┴─────┘
                            │                 │
                            └── query range ──┘
                      fan-out = 10 partitions × 2 buckets = 20 segments
                      FastAggregation.or(bm_0 … bm_19) → unified result
```

Each cell is **independent**: its own dict, its own bitmaps, its own offset range on one Kafka partition. A query over a time range fetches every cell intersecting that range across every partition and unions the per-segment bitmaps. Cross-partition distinct-count is correct as long as the member id is stable across partitions — which is the case for `raw_uint32` and `hash64`, and which is why `dict` member encoding is called out as a cross-segment limitation in the evaluator.

### A segment on disk

```
segments/seg_00000000000000000003_p00000_1777024800000_1777028400000/
│                                │└── partition (5 digits, zero-padded)
│                                └──── segment id (20 digits, zero-padded)
│
├── meta.json              {id, partition, startMs, endMs, recordCount,
│                           offsets: {0: {first, last}},
│                           schemaVersion, dimNames, metricNames}
│
├── dims/                  one .rbi per declared dimension
│   ├── country.rbi        [N:int] then N × ([valueId:int][size:int][roaring bytes])
│   ├── device.rbi            "FR" → valueId 0 → RoaringBitmap of member ids
│   ├── status.rbi            "DE" → valueId 1 → RoaringBitmap of member ids
│   └── path.rbi              ...
│
├── dicts/                 value ↔ id lookup for dict-encoded dims
│   ├── country.dict       "0\tFR\n1\tDE\n..."
│   └── device.dict
│
└── metrics/
    ├── distinctUsers.ull   [sliceCount:int] then N × (slice tuple + ULL bytes)
    ├── requestCount.count  [8-byte long]
    └── totalLatencyMs.sum  [8-byte long]
```

The `.rbi` wire format is portable Roaring, readable from any language with a Roaring port — the file is a list of `(valueId, bitmap)` pairs length-prefixed, nothing bespoke.

### The manifest

A single `manifest.json` at the root is the source of truth for *which segments exist*. It's updated atomically on every rollover via tmp-file + `ATOMIC_MOVE`.

```json
{
  "version": 1,
  "entries": [
    {
      "id": 3,
      "partition": 0,
      "startMs": 1777024800000,
      "endMs":   1777028400000,
      "recordCount": 333761,
      "dir": "seg_...p00000_1777024800000_1777028400000",
      "offsets":        { "0": { "first": 0, "last": 333760 } },
      "dimValueCounts": { "country": 10, "device": 3, "status": 9, "path": 10000 }
    },
    { "id": 9, "partition": 1, "startMs": 1777024800000, ... },
    ...
  ]
}
```

Why it exists: a query with `from=10h&to=15h` must not scan `segments/`. It filters `manifest.entries` on `(startMs < to) AND (endMs > from)` — plus an optional `partitions` subset — to get the list of directories to touch. Directory scan is O(list) and, on object storage, a paid operation per segment. Manifest lookup is O(1) once cached.

Startup path: `loadAll` reads the manifest if present and loads only the entries it lists; if the manifest is missing (legacy directory or corrupted write), it falls back to a directory scan and rebuilds the manifest as a side effect. So the manifest is advisory for correctness and authoritative for planning.

At SaaS scale, this is the object you hold in RAM per tenant. A topic with 10 partitions × 30d × 1h buckets produces ~7200 entries, roughly 1–2 MB of JSON, trivial to index in memory by `(partition, startMs)` for range pruning.

## The shape of a query

```
GET /query
    ?from=2026-04-24T00:00:00Z
    &to=2026-04-24T23:00:00Z
    &filter=country:FR,DE AND device:mobile AND NOT status:5xx
    &agg=cardinality
```

```json
{ "segments": 24, "matched": 48211, "result": 48211, "metric": "cardinality", "approx": false }
```

Under the hood: find the segments overlapping `[from, to)`, **fan out** filter evaluation across them (parallel on `ForkJoinPool` once `segments.size ≥ parallelThreshold`), each segment reduces the filter AST into a `RoaringBitmap` from its pre-built dim-value bitmaps, then **gather** via `FastAggregation.or` — the cache-aware multi-way union beats a sequential fold. For cardinality the result is `.cardinality()` of the merged bitmap; for ULL metrics we either merge pre-computed sketches (approximate, requires exact-slice-match) or rebuild exactly from the matched bitmap when `metric.field == member.field`.

```mermaid
flowchart TD
  Q[GET /query] --> P[parse filter AST<br/>AND / OR / NOT / IN]
  P --> F[segmentsOverlapping<br/>from..to]
  F -->|fan-out<br/>ForkJoinPool| R1["seg 1:<br/>reduce AST → bitmap"]
  F --> R2["seg 2:<br/>reduce AST → bitmap"]
  F --> R3["seg N:<br/>reduce AST → bitmap"]
  R1 --> G[FastAggregation.or<br/>cache-aware union]
  R2 --> G
  R3 --> G
  G --> A{agg}
  A -->|cardinality| U[".cardinality()"]
  A -->|count| SC[sum / stored counter]
  A -->|ULL metric| M["merge sketches (if sliceBy matches)<br/>OR exact via bitmap (if field == member)"]
  U --> O[JSON response]
  SC --> O
  M --> O
```

Concurrency with the writer: the consumer thread keeps ingesting into the open segment while queries run. Frozen segments are bitmap-immutable (no lock needed). The open segment is queried under a shared read-lock that clones each touched bitmap inside the lock window — the writer continues to mutate the live bitmaps under a per-bitmap `synchronized` guard, and the query operates on snapshots. Reads and writes don't serialize against each other; only `freeze()` (rollover) takes the write-lock and waits for in-flight reads to finish.

## Prior art, positioning

Druid, Pinot, ClickHouse, FeatureBase (ex-Pilosa) all use Roaring under the hood for posting lists — they're what you reach for when this sidecar is no longer enough. Tantivy / Lucene / Quickwit do the same inside search engines. Kafka Streams / ksqlDB occupy a different axis: streaming SQL transformations, not pre-aggregated indexes.

Closest cousin in spirit: **FeatureBase (Pilosa)**. Same mental model — "bitmap database" indexing declared dimensions for set-theoretic analytics, not full-text.

Often confused with us but genuinely different: **Quickwit**. Quickwit is a distributed search engine on Tantivy (Rust-Lucene) with Kafka sources; it scans posting lists and answers "find events matching". We are pre-aggregated per `(dim, value)` with the **member** as a first-class citizen, so we answer "how many **distinct users** match this filter" as one-to-three SIMD operations instead of a scan. Quickwit wins on log search, ad-hoc fields, and object-storage scale. We win on sub-millisecond distinct-cardinality on a declared, opinionated schema, from a single process.

The niche here: **single-binary, consumer-side, declarative, opinionated**. Not a Druid replacement. A pragmatic middle ground when spinning up an OLAP cluster is overkill and scanning Kafka is underkill.

## Status

POC implementation complete against `SPEC.md` milestones M1 through M11. Design doc is intentionally ahead of the code: several knobs (protobuf Schema Registry, zstd segment compression, multi-partition ownership on rebalance, dict snapshot cadence) are sketched but not load-bearing yet.

## License

TBD.

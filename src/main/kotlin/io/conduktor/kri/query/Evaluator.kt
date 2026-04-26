package io.conduktor.kri.query

import com.dynatrace.hash4j.distinctcount.UltraLogLog
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.Metric
import io.conduktor.kri.index.BitSlicedIndex
import io.conduktor.kri.index.DictEncoder
import io.conduktor.kri.index.RawUInt32Encoder
import io.conduktor.kri.index.Segment
import org.roaringbitmap.FastAggregation
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ForkJoinPool

data class QueryRequest(
    val from: Instant,
    val to: Instant,
    val filter: FilterAst,
    val agg: String,
)

data class QueryResponse(
    val segmentCount: Int,
    val matchedRecords: Long,
    val result: Any?,
    val metric: String,
    val approx: Boolean,
)

class Evaluator(
    private val cfg: IndexerConfig,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val poolLazy =
        lazy {
            val p = cfg.query.parallelism ?: Runtime.getRuntime().availableProcessors()
            if (p <= 1) ForkJoinPool(1) else ForkJoinPool(p)
        }
    private val pool: ForkJoinPool by poolLazy

    private fun evalPerSegment(
        segments: List<Segment>,
        filter: FilterAst,
    ): List<RoaringBitmap> {
        val threshold = cfg.query.parallelThreshold
        if (segments.size < threshold || cfg.query.parallelism == 1) {
            return segments.map { evalFilter(it, filter) }
        }
        // Parallel fan-out on a dedicated pool so we don't pollute commonPool.
        return pool
            .submit<List<RoaringBitmap>> {
                segments
                    .parallelStream()
                    .map { evalFilter(it, filter) }
                    .toList()
            }.get()
    }

    fun evaluate(
        segments: List<Segment>,
        req: QueryRequest,
    ): QueryResponse {
        val metric = cfg.metrics.firstOrNull { it.name == req.agg }

        when {
            req.agg == "cardinality" -> {
                val perSegBitmaps = evalPerSegment(segments, req.filter)
                // FastAggregation.or is cache-aware; vastly better than sequential .or() fold.
                // Cross-segment cardinality: uint32 member ids are stable across segments only for
                // raw_uint32 and hash64 encodings. For dict encoding the union is NOT equivalent
                // to a distinct-count (ids differ per segment dict) — noted limitation for v1.
                val unionBitmap =
                    if (perSegBitmaps.isEmpty()) {
                        RoaringBitmap()
                    } else {
                        FastAggregation.or(*perSegBitmaps.toTypedArray())
                    }
                return QueryResponse(
                    segmentCount = segments.size,
                    matchedRecords = perSegBitmaps.sumOf { it.longCardinality.toLong() },
                    result = unionBitmap.longCardinality.toLong(),
                    metric = "cardinality",
                    approx = false,
                )
            }

            req.agg == "count" -> {
                val matched = evalPerSegment(segments, req.filter)
                val total = matched.sumOf { it.longCardinality.toLong() }
                return QueryResponse(segments.size, total, total, "count", false)
            }

            metric != null && metric.type == Metric.Type.ULL -> {
                return aggregateUll(segments, req, metric)
            }

            metric != null && metric.type == Metric.Type.COUNT -> {
                val total = segments.sumOf { it.counters[metric.name]?.get() ?: 0L }
                return QueryResponse(segments.size, total, total, metric.name, false)
            }

            metric != null && metric.type == Metric.Type.SUM -> {
                val total = segments.sumOf { it.sums[metric.name]?.get() ?: 0L }
                return QueryResponse(segments.size, total, total, metric.name, false)
            }

            else -> throw IllegalArgumentException("unknown agg: ${req.agg}")
        }
    }

    private fun aggregateUll(
        segments: List<Segment>,
        req: QueryRequest,
        metric: Metric,
    ): QueryResponse {
        val precision = metric.precision ?: 12
        // Case: metric.field == member.field → rebuild from matched bitmap = exact member ids, but
        //       sketches cover all records in segment — so with a filter, we must use filtered bitmaps
        //       and fold to the sketch. Requires mapping member_id back to sketchable input.
        //       Simplest route for v1 when sliceBy is empty and there's no filter: use segment sketches.
        //       Otherwise fallback to cardinality over bitmaps if member == metric.field.
        val hasFilter = req.filter !is FilterAst.True
        val sliceBy = metric.sliceBy.orEmpty()

        if (!hasFilter && sliceBy.isEmpty()) {
            // Merge segment sketches directly.
            val merged =
                segments
                    .mapNotNull { it.ullSketches[metric.name]?.get(emptyList()) }
                    .fold(null as UltraLogLog?) { acc, sk ->
                        if (acc == null) UltraLogLog.wrap(sk.state.copyOf()) else UltraLogLog.merge(acc, sk)
                    }
            val est = merged?.distinctCountEstimate ?: 0.0
            return QueryResponse(segments.size, -1L, est, metric.name, true)
        }

        // Case with filter: if metric.field == member.field, derive from bitmap (exact).
        if (metric.field == cfg.member.field) {
            val matched = evalPerSegment(segments, req.filter)
            val union =
                if (matched.isEmpty()) {
                    RoaringBitmap()
                } else {
                    FastAggregation.or(*matched.toTypedArray())
                }
            return QueryResponse(segments.size, union.longCardinality.toLong(), union.longCardinality.toLong(), metric.name, false)
        }

        // Otherwise v1 restriction: require exact sliceBy match.
        throw IllegalArgumentException(
            "cannot evaluate metric '${metric.name}' with filter/slice mismatch. " +
                "Either remove filter, or query its exact sliceBy tuple, or use metric.field==member.field.",
        )
    }

    /** Union of per-segment filtered bitmaps — the raw membership result before aggregation. */
    fun evalMergedBitmap(
        segments: List<Segment>,
        filter: FilterAst,
    ): RoaringBitmap {
        val bitmaps = evalPerSegment(segments, filter)
        return if (bitmaps.isEmpty()) RoaringBitmap() else FastAggregation.or(*bitmaps.toTypedArray())
    }

    /**
     * Reduces the filter to a RoaringBitmap within one segment, in CANONICAL memberId space.
     *
     * Uses reorderedDims + BSI when available (transparent perf optimization). The result is
     * always translated back to canonical so cross-segment merges (FastAggregation.or) and
     * facets (which AND against canonical seg.dims) remain correct.
     *
     * Use [evalFilterCanonical] for a baseline that ignores reordered/BSI (used by the
     * multi-model comparison endpoint).
     */
    fun evalFilter(
        seg: Segment,
        ast: FilterAst,
    ): RoaringBitmap {
        val reordered = seg.reorderedDims
        val perm = seg.memberPermutation
        return if (reordered != null && perm != null) {
            val raw =
                evalFilterOn(
                    seg,
                    ast,
                    reordered,
                    seg.reorderedMemberUniverse() ?: RoaringBitmap(),
                    useReorderedSpace = true,
                    bsiAllowed = true,
                )
            perm.translateBack(raw)
        } else {
            evalFilterOn(seg, ast, seg.dims, seg.memberUniverse(), useReorderedSpace = false, bsiAllowed = true)
        }
    }

    /**
     * Pure baseline — ignores reorderedDims AND bsi. Always reads from seg.dims with the
     * legacy linear-scan range eval. Kept so the multi-model bench can compare against an
     * unoptimized Roaring channel.
     */
    fun evalFilterCanonical(
        seg: Segment,
        ast: FilterAst,
    ): RoaringBitmap = evalFilterOn(seg, ast, seg.dims, seg.memberUniverse(), useReorderedSpace = false, bsiAllowed = false)

    /** Internal eval driver. `source` is either canonical seg.dims or seg.reorderedDims. */
    private fun evalFilterOn(
        seg: Segment,
        ast: FilterAst,
        source: Map<String, Map<Int, RoaringBitmap>>,
        universe: RoaringBitmap,
        useReorderedSpace: Boolean,
        bsiAllowed: Boolean,
    ): RoaringBitmap =
        when (ast) {
            FilterAst.True -> universe
            is FilterAst.Predicate -> predicateOn(seg, ast, source)
            is FilterAst.Range -> rangeOn(seg, ast, source, useReorderedSpace, bsiAllowed)
            is FilterAst.And -> {
                val a = evalFilterOn(seg, ast.left, source, universe, useReorderedSpace, bsiAllowed)
                val b = evalFilterOn(seg, ast.right, source, universe, useReorderedSpace, bsiAllowed)
                RoaringBitmap.and(a, b)
            }
            is FilterAst.Or -> {
                val a = evalFilterOn(seg, ast.left, source, universe, useReorderedSpace, bsiAllowed)
                val b = evalFilterOn(seg, ast.right, source, universe, useReorderedSpace, bsiAllowed)
                RoaringBitmap.or(a, b)
            }
            is FilterAst.Not -> {
                val inner = evalFilterOn(seg, ast.inner, source, universe, useReorderedSpace, bsiAllowed)
                RoaringBitmap.andNot(universe, inner)
            }
        }

    fun shutdown() {
        if (poolLazy.isInitialized()) pool.shutdown()
    }

    /**
     * Range eval. Dispatches to BSI when allowed AND seg.bsi[dim] is built; otherwise scans
     * the value map. BSI is keyed in canonical memberId space — when source is reordered,
     * translate forward.
     */
    private fun rangeOn(
        seg: Segment,
        r: FilterAst.Range,
        source: Map<String, Map<Int, RoaringBitmap>>,
        useReorderedSpace: Boolean,
        bsiAllowed: Boolean,
    ): RoaringBitmap {
        if (bsiAllowed) {
            seg.bsi[r.dim]?.let { bsi ->
                val canonical = rangeViaBsi(bsi, r)
                return if (useReorderedSpace) seg.memberPermutation!!.translateForward(canonical) else canonical
            }
        }
        val enc = seg.dimEncoder(r.dim) ?: return RoaringBitmap()
        if (enc !is RawUInt32Encoder) {
            log.warn("range filter on non-uint32 dim '{}' — returning empty", r.dim)
            return RoaringBitmap()
        }
        val valueMap = source[r.dim] ?: return RoaringBitmap()
        val acc = RoaringBitmap()
        for ((rawId, bm) in valueMap) {
            val v = rawId.toLong() and 0xFFFF_FFFFL
            val inRange =
                when {
                    r.lo != null && r.hi != null ->
                        (if (r.loInclusive) v >= r.lo else v > r.lo) &&
                            (if (r.hiInclusive) v <= r.hi else v < r.hi)
                    r.lo != null -> if (r.loInclusive) v >= r.lo else v > r.lo
                    r.hi != null -> if (r.hiInclusive) v <= r.hi else v < r.hi
                    else -> false
                }
            if (inRange) acc.or(bm)
        }
        return acc
    }

    /** O'Neil BSI compare wrapper — maps a Range AST onto BSI primitives. */
    private fun rangeViaBsi(
        bsi: BitSlicedIndex,
        r: FilterAst.Range,
    ): RoaringBitmap {
        val lo = r.lo
        val hi = r.hi
        return when {
            lo != null && hi != null -> {
                val loB = if (r.loInclusive) bsi.ge(lo) else bsi.gt(lo)
                val hiB = if (r.hiInclusive) bsi.le(hi) else bsi.lt(hi)
                RoaringBitmap.and(loB, hiB)
            }
            lo != null -> if (r.loInclusive) bsi.ge(lo) else bsi.gt(lo)
            hi != null -> if (r.hiInclusive) bsi.le(hi) else bsi.lt(hi)
            else -> RoaringBitmap()
        }
    }

    private fun predicateOn(
        seg: Segment,
        p: FilterAst.Predicate,
        source: Map<String, Map<Int, RoaringBitmap>>,
    ): RoaringBitmap {
        val enc = seg.dimEncoder(p.dim) ?: return RoaringBitmap()
        val ids = p.values.mapNotNull { v -> encodeValue(enc, v) }
        if (ids.isEmpty()) return RoaringBitmap()
        val parts = ids.mapNotNull { id -> source[p.dim]?.get(id)?.clone() }
        if (parts.isEmpty()) return RoaringBitmap()
        val acc = RoaringBitmap()
        parts.forEach { acc.or(it) }
        return acc
    }

    private fun encodeValue(
        enc: io.conduktor.kri.index.DimEncoder,
        v: String,
    ): Int? =
        when (enc) {
            is DictEncoder -> enc.dict().lookup(v)
            is RawUInt32Encoder -> v.toLongOrNull()?.takeIf { it in 0..0xFFFF_FFFFL }?.toInt()
            else -> {
                // Hash encoders — apply the same transformation used at ingest.
                when (val r = enc.encode(v)) {
                    is io.conduktor.kri.index.EncodeResult.Ok -> r.id
                    else -> null
                }
            }
        }
}

package io.conduktor.kri.query

import com.dynatrace.hash4j.distinctcount.UltraLogLog
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.Metric
import io.conduktor.kri.index.DictEncoder
import io.conduktor.kri.index.RawUInt32Encoder
import io.conduktor.kri.index.Segment
import org.roaringbitmap.FastAggregation
import org.roaringbitmap.RoaringBitmap
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

    /**
     * Reduces the filter to a RoaringBitmap within one segment.
     * Uses [Segment.dimValueBitmap] / [Segment.memberUniverse] which snapshot (clone) bitmaps
     * on open segments under the segment read-lock; frozen segments return bitmaps directly.
     * Result is a fresh bitmap owned by the caller — safe to hand to FastAggregation.or.
     */
    fun evalFilter(
        seg: Segment,
        ast: FilterAst,
    ): RoaringBitmap =
        when (ast) {
            FilterAst.True -> seg.memberUniverse()
            is FilterAst.Predicate -> predicateToBitmap(seg, ast)
            is FilterAst.And -> {
                val a = evalFilter(seg, ast.left)
                val b = evalFilter(seg, ast.right)
                RoaringBitmap.and(a, b)
            }
            is FilterAst.Or -> {
                val a = evalFilter(seg, ast.left)
                val b = evalFilter(seg, ast.right)
                RoaringBitmap.or(a, b)
            }
            is FilterAst.Not -> {
                val universe = seg.memberUniverse()
                val inner = evalFilter(seg, ast.inner)
                RoaringBitmap.andNot(universe, inner)
            }
        }

    fun shutdown() {
        if (poolLazy.isInitialized()) pool.shutdown()
    }

    private fun predicateToBitmap(
        seg: Segment,
        p: FilterAst.Predicate,
    ): RoaringBitmap {
        val enc = seg.dimEncoder(p.dim) ?: return RoaringBitmap()
        val ids = p.values.mapNotNull { v -> encodeValue(enc, v) }
        if (ids.isEmpty()) return RoaringBitmap()
        val parts = ids.mapNotNull { id -> seg.dimValueBitmap(p.dim, id) }
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

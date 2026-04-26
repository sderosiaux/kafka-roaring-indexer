package io.conduktor.kri.query

import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.index.BitSlicedIndex
import io.conduktor.kri.index.IndexModelKind
import io.conduktor.kri.index.JointProfileIndex
import io.conduktor.kri.index.ModelResult
import io.conduktor.kri.index.RawUInt32Encoder
import io.conduktor.kri.index.Segment
import io.conduktor.kri.index.ThetaSampleIndex
import org.roaringbitmap.FastAggregation
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory

/**
 * Evaluates the same query through every requested IndexModelKind so the user can compare
 * timings, exact-vs-approx, and supported-vs-not side-by-side.
 *
 * Each model that cannot answer a given (filter, agg) returns supported=false with a message.
 * Currently only `agg = cardinality` is supported for non-Roaring models — they target
 * "distinct members under filter", which is the original benchmark target.
 */
class MultiModelEvaluator(
    private val cfg: IndexerConfig,
    private val rev: Evaluator,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun evaluate(
        segments: List<Segment>,
        req: QueryRequest,
        models: List<IndexModelKind>,
    ): List<ModelResult> = models.map { runOne(segments, req, it) }

    private fun runOne(
        segments: List<Segment>,
        req: QueryRequest,
        model: IndexModelKind,
    ): ModelResult {
        val t0 = System.nanoTime()
        return try {
            when (model) {
                IndexModelKind.ROARING -> roaringResult(segments, req, t0)
                IndexModelKind.ROARING_REORDERED -> roaringReorderedResult(segments, req, t0)
                IndexModelKind.BSI -> bsiResult(segments, req, t0)
                IndexModelKind.THETA -> thetaResult(segments, req, t0)
                IndexModelKind.JOINT_PROFILE -> jointProfileResult(segments, req, t0)
            }
        } catch (e: Exception) {
            log.warn("model {} failed: {}", model, e.message)
            ModelResult(model, supported = false, value = null, approx = false, timeNs = System.nanoTime() - t0, error = e.message)
        }
    }

    // ── ROARING (true canonical baseline — bypasses reordered + BSI) ──────────

    private fun roaringResult(
        segments: List<Segment>,
        req: QueryRequest,
        t0: Long,
    ): ModelResult {
        if (req.agg != "cardinality" && req.agg != "count") {
            // For metric aggs we still defer to the regular evaluator (which is canonical
            // for non-cardinality paths anyway).
            val resp = rev.evaluate(segments, req)
            return ModelResult(IndexModelKind.ROARING, true, resp.result, resp.approx, System.nanoTime() - t0)
        }
        val perSeg = segments.map { rev.evalFilterCanonical(it, req.filter) }
        val union = if (perSeg.isEmpty()) RoaringBitmap() else FastAggregation.or(*perSeg.toTypedArray())
        val result =
            if (req.agg == "cardinality") union.longCardinality.toLong() else perSeg.sumOf { it.longCardinality.toLong() }
        return ModelResult(IndexModelKind.ROARING, true, result, false, System.nanoTime() - t0)
    }

    // ── ROARING REORDERED ──────────────────────────────────────────────────────

    private fun roaringReorderedResult(
        segments: List<Segment>,
        req: QueryRequest,
        t0: Long,
    ): ModelResult {
        if (req.agg != "cardinality" && req.agg != "count") {
            return ModelResult(
                IndexModelKind.ROARING_REORDERED,
                false,
                null,
                false,
                System.nanoTime() - t0,
                "only cardinality/count supported",
            )
        }
        val missing = segments.firstOrNull { it.reorderedDims == null }
        if (missing != null) {
            return ModelResult(
                IndexModelKind.ROARING_REORDERED,
                false,
                null,
                false,
                System.nanoTime() - t0,
                "segment ${missing.id} has no reordered dims (experiment disabled)",
            )
        }
        val perSeg = segments.map { evalRoaringOnAlt(it, req.filter, it.reorderedDims!!) }
        val union = if (perSeg.isEmpty()) RoaringBitmap() else FastAggregation.or(*perSeg.toTypedArray())
        val result =
            if (req.agg == "cardinality") union.longCardinality.toLong() else perSeg.sumOf { it.longCardinality.toLong() }
        return ModelResult(IndexModelKind.ROARING_REORDERED, true, result, false, System.nanoTime() - t0)
    }

    /** Same logic as Evaluator.evalFilter but reads from a caller-supplied `dims` map. */
    private fun evalRoaringOnAlt(
        seg: Segment,
        ast: FilterAst,
        altDims: MutableMap<String, MutableMap<Int, RoaringBitmap>>,
    ): RoaringBitmap {
        fun universe(): RoaringBitmap {
            val all = RoaringBitmap()
            altDims.values.forEach { vm -> vm.values.forEach { all.or(it) } }
            return all
        }
        return when (ast) {
            FilterAst.True -> universe()
            is FilterAst.Predicate -> {
                val enc = seg.dimEncoder(ast.dim) ?: return RoaringBitmap()
                val ids = ast.values.mapNotNull { encodeForFilter(enc, it) }
                val parts = ids.mapNotNull { altDims[ast.dim]?.get(it)?.clone() }
                if (parts.isEmpty()) RoaringBitmap() else parts.reduce { a, b -> a.apply { or(b) } }
            }
            is FilterAst.Range -> {
                val enc = seg.dimEncoder(ast.dim)
                if (enc !is RawUInt32Encoder) return RoaringBitmap()
                val acc = RoaringBitmap()
                altDims[ast.dim]?.forEach { (rawId, bm) ->
                    val v = rawId.toLong() and 0xFFFF_FFFFL
                    val inRange =
                        when {
                            ast.lo != null && ast.hi != null ->
                                (if (ast.loInclusive) v >= ast.lo else v > ast.lo) &&
                                    (if (ast.hiInclusive) v <= ast.hi else v < ast.hi)
                            ast.lo != null -> if (ast.loInclusive) v >= ast.lo else v > ast.lo
                            ast.hi != null -> if (ast.hiInclusive) v <= ast.hi else v < ast.hi
                            else -> false
                        }
                    if (inRange) acc.or(bm)
                }
                acc
            }
            is FilterAst.And -> RoaringBitmap.and(evalRoaringOnAlt(seg, ast.left, altDims), evalRoaringOnAlt(seg, ast.right, altDims))
            is FilterAst.Or -> RoaringBitmap.or(evalRoaringOnAlt(seg, ast.left, altDims), evalRoaringOnAlt(seg, ast.right, altDims))
            is FilterAst.Not -> RoaringBitmap.andNot(universe(), evalRoaringOnAlt(seg, ast.inner, altDims))
        }
    }

    // ── BSI ────────────────────────────────────────────────────────────────────

    private fun bsiResult(
        segments: List<Segment>,
        req: QueryRequest,
        t0: Long,
    ): ModelResult {
        if (req.agg != "cardinality" && req.agg != "count") {
            return ModelResult(IndexModelKind.BSI, false, null, false, System.nanoTime() - t0, "only cardinality/count")
        }
        val missingDims =
            collectRangeDims(req.filter).filter { dim ->
                segments.any { it.bsi[dim] == null }
            }
        if (missingDims.isNotEmpty()) {
            return ModelResult(
                IndexModelKind.BSI,
                false,
                null,
                false,
                System.nanoTime() - t0,
                "missing BSI for dim(s): ${missingDims.joinToString(",")}",
            )
        }
        val perSeg = segments.map { evalBsiHybrid(it, req.filter) }
        val union = if (perSeg.isEmpty()) RoaringBitmap() else FastAggregation.or(*perSeg.toTypedArray())
        val result =
            if (req.agg == "cardinality") union.longCardinality.toLong() else perSeg.sumOf { it.longCardinality.toLong() }
        return ModelResult(IndexModelKind.BSI, true, result, false, System.nanoTime() - t0)
    }

    /**
     * Recursively walks the filter; Range predicates hit BSI, everything else uses *canonical*
     * Roaring (NOT the reordered path) so AND/OR composition stays in canonical-id space and
     * we don't pay reordered+translate overhead per leaf.
     */
    private fun evalBsiHybrid(
        seg: Segment,
        ast: FilterAst,
    ): RoaringBitmap =
        when (ast) {
            is FilterAst.Range -> {
                val bsi = seg.bsi[ast.dim] ?: error("BSI missing for dim ${ast.dim}")
                rangeViaBsi(bsi, ast)
            }
            FilterAst.True, is FilterAst.Predicate -> rev.evalFilterCanonical(seg, ast)
            is FilterAst.And -> RoaringBitmap.and(evalBsiHybrid(seg, ast.left), evalBsiHybrid(seg, ast.right))
            is FilterAst.Or -> RoaringBitmap.or(evalBsiHybrid(seg, ast.left), evalBsiHybrid(seg, ast.right))
            is FilterAst.Not -> {
                val universe = seg.memberUniverse()
                RoaringBitmap.andNot(universe, evalBsiHybrid(seg, ast.inner))
            }
        }

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

    private fun collectRangeDims(ast: FilterAst): Set<String> =
        when (ast) {
            is FilterAst.Range -> setOf(ast.dim)
            is FilterAst.And -> collectRangeDims(ast.left) + collectRangeDims(ast.right)
            is FilterAst.Or -> collectRangeDims(ast.left) + collectRangeDims(ast.right)
            is FilterAst.Not -> collectRangeDims(ast.inner)
            else -> emptySet()
        }

    // ── THETA ──────────────────────────────────────────────────────────────────

    private fun thetaResult(
        segments: List<Segment>,
        req: QueryRequest,
        t0: Long,
    ): ModelResult {
        if (req.agg != "cardinality") {
            return ModelResult(IndexModelKind.THETA, false, null, false, System.nanoTime() - t0, "theta supports cardinality only")
        }
        val first = segments.firstOrNull { it.theta != null }
        if (first == null || segments.any { it.theta == null }) {
            return ModelResult(IndexModelKind.THETA, false, null, false, System.nanoTime() - t0, "theta sample not built on all segments")
        }
        val rate = first.theta!!.sampleRate
        if (containsRange(req.filter)) {
            return ModelResult(IndexModelKind.THETA, false, null, false, System.nanoTime() - t0, "theta does not support range predicates")
        }
        // Same theta across segments → union of samples is well-defined.
        val perSeg = segments.map { evalOnTheta(it, it.theta!!, req.filter) }
        val union = if (perSeg.isEmpty()) RoaringBitmap() else FastAggregation.or(*perSeg.toTypedArray())
        val sampledCount = union.longCardinality.toDouble()
        val estimate = (sampledCount / rate).toLong()
        log.debug("theta estimate: sampled={} rate={} → {}", sampledCount, rate, estimate)
        return ModelResult(IndexModelKind.THETA, true, estimate, true, System.nanoTime() - t0)
    }

    private fun evalOnTheta(
        seg: Segment,
        idx: ThetaSampleIndex,
        ast: FilterAst,
    ): RoaringBitmap =
        when (ast) {
            FilterAst.True -> idx.sampledUniverse.clone()
            is FilterAst.Predicate -> {
                val enc = seg.dimEncoder(ast.dim) ?: return RoaringBitmap()
                val ids = ast.values.mapNotNull { encodeForFilter(enc, it) }
                val parts = ids.mapNotNull { idx.dimValueBitmap(ast.dim, it)?.clone() }
                if (parts.isEmpty()) RoaringBitmap() else parts.reduce { a, b -> a.apply { or(b) } }
            }
            is FilterAst.Range -> error("theta does not support range predicates")
            is FilterAst.And -> RoaringBitmap.and(evalOnTheta(seg, idx, ast.left), evalOnTheta(seg, idx, ast.right))
            is FilterAst.Or -> RoaringBitmap.or(evalOnTheta(seg, idx, ast.left), evalOnTheta(seg, idx, ast.right))
            is FilterAst.Not -> RoaringBitmap.andNot(idx.sampledUniverse.clone(), evalOnTheta(seg, idx, ast.inner))
        }

    // ── JOINT PROFILE ──────────────────────────────────────────────────────────

    private fun jointProfileResult(
        segments: List<Segment>,
        req: QueryRequest,
        t0: Long,
    ): ModelResult {
        if (req.agg != "cardinality" && req.agg != "count") {
            return ModelResult(IndexModelKind.JOINT_PROFILE, false, null, false, System.nanoTime() - t0, "cardinality/count only")
        }
        if (containsRange(req.filter)) {
            return ModelResult(
                IndexModelKind.JOINT_PROFILE,
                false,
                null,
                false,
                System.nanoTime() - t0,
                "joint-profile does not support range predicates",
            )
        }
        val missing = segments.firstOrNull { it.jointProfile == null }
        if (missing != null) {
            return ModelResult(
                IndexModelKind.JOINT_PROFILE,
                false,
                null,
                false,
                System.nanoTime() - t0,
                "segment ${missing.id} has no joint-profile (experiment disabled)",
            )
        }
        val perSeg = segments.map { evalJointProfile(it, it.jointProfile!!, req.filter) }
        val union = if (perSeg.isEmpty()) RoaringBitmap() else FastAggregation.or(*perSeg.toTypedArray())
        val result =
            if (req.agg == "cardinality") union.longCardinality.toLong() else perSeg.sumOf { it.longCardinality.toLong() }
        return ModelResult(IndexModelKind.JOINT_PROFILE, true, result, false, System.nanoTime() - t0)
    }

    /** Walk filter in profile space, then expand once at the end. */
    private fun evalJointProfile(
        seg: Segment,
        jp: JointProfileIndex,
        ast: FilterAst,
    ): RoaringBitmap {
        val profileMatch = profileBitmap(seg, jp, ast)
        return jp.expandProfilesToMembers(profileMatch)
    }

    private fun profileBitmap(
        seg: Segment,
        jp: JointProfileIndex,
        ast: FilterAst,
    ): RoaringBitmap =
        when (ast) {
            FilterAst.True -> jp.profileUniverse.clone()
            is FilterAst.Predicate -> {
                val enc = seg.dimEncoder(ast.dim) ?: return RoaringBitmap()
                val ids = ast.values.mapNotNull { encodeForFilter(enc, it) }
                val parts =
                    ids.mapNotNull { vid ->
                        val atomId = jp.atomFor(ast.dim, vid)
                        if (atomId == null) null else jp.atomToProfiles[atomId]
                    }
                if (parts.isEmpty()) RoaringBitmap() else parts.fold(RoaringBitmap()) { acc, b -> acc.apply { or(b) } }
            }
            is FilterAst.Range -> error("range filter unsupported in joint-profile model")
            is FilterAst.And -> RoaringBitmap.and(profileBitmap(seg, jp, ast.left), profileBitmap(seg, jp, ast.right))
            is FilterAst.Or -> RoaringBitmap.or(profileBitmap(seg, jp, ast.left), profileBitmap(seg, jp, ast.right))
            is FilterAst.Not -> RoaringBitmap.andNot(jp.profileUniverse.clone(), profileBitmap(seg, jp, ast.inner))
        }

    private fun containsRange(ast: FilterAst): Boolean =
        when (ast) {
            is FilterAst.Range -> true
            is FilterAst.And -> containsRange(ast.left) || containsRange(ast.right)
            is FilterAst.Or -> containsRange(ast.left) || containsRange(ast.right)
            is FilterAst.Not -> containsRange(ast.inner)
            else -> false
        }

    private fun encodeForFilter(
        enc: io.conduktor.kri.index.DimEncoder,
        v: String,
    ): Int? =
        when (enc) {
            is io.conduktor.kri.index.DictEncoder -> enc.dict().lookup(v)
            is RawUInt32Encoder -> v.toLongOrNull()?.takeIf { it in 0..0xFFFF_FFFFL }?.toInt()
            else ->
                when (val r = enc.encode(v)) {
                    is io.conduktor.kri.index.EncodeResult.Ok -> r.id
                    else -> null
                }
        }
}

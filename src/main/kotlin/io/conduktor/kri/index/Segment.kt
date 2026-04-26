package io.conduktor.kri.index

import com.dynatrace.hash4j.distinctcount.UltraLogLog
import io.conduktor.kri.config.Dimension
import io.conduktor.kri.config.ExperimentalModels
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.Metric
import org.roaringbitmap.RoaringBitmap
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/** Immutable once frozen. Before freeze, thread-safe for concurrent add(). */
class Segment(
    val id: Long,
    val partition: Int,
    val tStart: Instant,
    val tEnd: Instant,
    private val dimSpecs: List<Dimension>,
    private val metricSpecs: List<Metric>,
    private val bucketers: Map<String, Bucketer>,
    private val memberEncoder: MemberEncoder,
    private val dimEncoders: Map<String, DimEncoder>,
    private val memberFieldName: String,
    private val metricFields: Map<String, String?>,
    private val experimental: ExperimentalModels = ExperimentalModels(),
) {
    private val lock = ReentrantReadWriteLock()

    @Volatile var frozen: Boolean = false
        private set

    @Volatile private var frozenUniverse: RoaringBitmap? = null

    /** dimName → (valueId → bitmap(members)). Both levels use ConcurrentHashMap for safe reads during writes. */
    val dims: MutableMap<String, MutableMap<Int, RoaringBitmap>> = ConcurrentHashMap()

    /** metricName → (slice → sketch/state) */
    val ullSketches: MutableMap<String, MutableMap<List<Int>, UltraLogLog>> = ConcurrentHashMap()
    val counters: MutableMap<String, AtomicLong> = ConcurrentHashMap()
    val sums: MutableMap<String, AtomicLong> = ConcurrentHashMap()

    /** partition → (first, last) offsets */
    val offsets: MutableMap<Int, LongRange> = ConcurrentHashMap()
    val recordCount = AtomicLong(0)

    // ── Parallel index models (opt-in via ExperimentalModels). Coexist with dims/ullSketches.

    /** dimName → BitSlicedIndex over the raw uint32 numeric value. Built online during add(). */
    val bsi: MutableMap<String, BitSlicedIndex> = ConcurrentHashMap()

    /** Per-segment theta sample of memberIds + parallel dim bitmaps over the sampled subset. */
    @Volatile var theta: ThetaSampleIndex? = null
        private set

    /** Built at freeze when [ExperimentalModels.jointProfile] is on. */
    @Volatile var jointProfile: JointProfileIndex? = null
        private set

    /** Built at freeze when [ExperimentalModels.reorderMembers] is on. */
    @Volatile var memberPermutation: MemberPermutation? = null
        private set

    /** Built at freeze when [ExperimentalModels.reorderMembers] is on. Same shape as [dims]. */
    @Volatile var reorderedDims: MutableMap<String, MutableMap<Int, RoaringBitmap>>? = null
        private set

    /** OR of all reordered dim bitmaps. Computed lazily and cached. Null if no reordering. */
    @Volatile private var reorderedUniverseCache: RoaringBitmap? = null

    init {
        dimSpecs.forEach { dims[it.name] = ConcurrentHashMap() }
        metricSpecs.forEach { m ->
            when (m.type) {
                Metric.Type.ULL -> ullSketches[m.name] = ConcurrentHashMap()
                Metric.Type.COUNT -> counters[m.name] = AtomicLong(0)
                Metric.Type.SUM -> sums[m.name] = AtomicLong(0)
                else -> { /* hll/cpc: omitted in v1 */ }
            }
        }
        // Parallel models — only initialize state for what's enabled.
        experimental.bsiDims.forEach { name ->
            if (dimSpecs.any { it.name == name }) bsi[name] = BitSlicedIndex(bitWidth = 32)
        }
        if (experimental.thetaSample) {
            theta = ThetaSampleIndex(theta = ThetaSampleIndex.thetaFor(experimental.thetaSampleRate))
        }
    }

    fun experimentalConfig(): ExperimentalModels = experimental

    fun dimSpecs(): List<Dimension> = dimSpecs

    fun fieldFor(dimName: String): String? = dimSpecs.firstOrNull { it.name == dimName }?.field

    fun add(
        partition: Int,
        offset: Long,
        rawFields: Map<String, Any?>,
    ): AddResult {
        if (frozen) return AddResult.RejectedFrozen

        val memberRaw = rawFields[memberFieldName]
        val memberId = memberEncoder.encode(memberRaw) ?: return AddResult.MemberMissing

        val overflows = mutableListOf<String>()

        // Track atoms that hit the canonical path so we can mirror them into the theta sample.
        val acceptedAtoms = if (theta != null) ArrayList<Pair<String, Int>>(dimSpecs.size) else null

        lock.read {
            dimSpecs.forEach { d ->
                val enc = dimEncoders.getValue(d.name)
                val rawValue = rawFields[d.field]
                val coerced =
                    d.bucket?.let { bucketers[d.name] }?.let { b ->
                        val numeric = (rawValue as? Number)?.toDouble() ?: (rawValue as? String)?.toDoubleOrNull()
                        numeric?.let { b.bucket(it) }
                    } ?: rawValue
                when (val r = enc.encode(coerced)) {
                    is EncodeResult.Ok -> {
                        val valueMap = dims.getValue(d.name)
                        val bm = synchronized(valueMap) { valueMap.getOrPut(r.id) { RoaringBitmap() } }
                        synchronized(bm) { bm.add(memberId) }
                        acceptedAtoms?.add(d.name to r.id)
                    }
                    EncodeResult.Overflow -> overflows.add(d.name)
                    EncodeResult.Skip -> { /* silently skip this dim */ }
                }

                // Parallel BSI path: bypasses bucketing — indexes the raw uint32 numeric value.
                bsi[d.name]?.let { bsiIdx ->
                    val numeric = (rawValue as? Number)?.toLong() ?: (rawValue as? String)?.toLongOrNull()
                    if (numeric != null && numeric in 0..0xFFFF_FFFFL) {
                        synchronized(bsiIdx) { bsiIdx.set(memberId, numeric) }
                    }
                }
            }

            metricSpecs.forEach { m ->
                when (m.type) {
                    Metric.Type.ULL -> {
                        val fieldName = metricFields[m.name] ?: return@forEach
                        val raw = rawFields[fieldName] ?: return@forEach
                        val slice =
                            m.sliceBy?.map { s ->
                                val sEnc = dimEncoders[s] ?: return@forEach
                                val r = sEnc.encode(rawFields[dimSpecs.first { it.name == s }.field])
                                if (r is EncodeResult.Ok) r.id else return@forEach
                            } ?: emptyList()
                        val sliceMap = ullSketches.getValue(m.name)
                        val sk =
                            synchronized(sliceMap) {
                                sliceMap.getOrPut(slice) { UltraLogLog.create(m.precision ?: 12) }
                            }
                        val h =
                            com.dynatrace.hash4j.hashing.Hashing
                                .komihash5_0()
                                .hashBytesToLong(raw.toString().toByteArray())
                        synchronized(sk) { sk.add(h) }
                    }
                    Metric.Type.COUNT -> counters.getValue(m.name).incrementAndGet()
                    Metric.Type.SUM -> {
                        val fieldName = metricFields[m.name] ?: return@forEach
                        val v = (rawFields[fieldName] as? Number)?.toLong() ?: return@forEach
                        sums.getValue(m.name).addAndGet(v)
                    }
                    else -> {}
                }
            }
        }

        // Mirror this record into the theta sample iff hash(memberId) < theta.
        // Done outside the dim loop so all atoms are known + threshold check happens once.
        theta?.let { th -> acceptedAtoms?.let { atoms -> th.maybeAdd(memberId, atoms) } }

        recordCount.incrementAndGet()
        offsets.compute(partition) { _, prev ->
            if (prev == null) offset..offset else minOf(prev.first, offset)..maxOf(prev.last, offset)
        }

        return if (overflows.isEmpty()) AddResult.Ok else AddResult.Overflow(overflows)
    }

    fun freeze() {
        lock.write {
            if (frozen) return
            dims.values.forEach { vm -> vm.values.forEach { it.runOptimize() } }
            val universe = RoaringBitmap()
            dims.values.forEach { vm -> vm.values.forEach { universe.or(it) } }
            frozenUniverse = universe

            // Parallel models: optimize then build derived structures.
            // Skip rebuild if state was already attached (e.g. by SegmentIO.read).
            bsi.values.forEach { it.runOptimize() }
            theta?.runOptimize()
            if (experimental.reorderMembers && memberPermutation == null) {
                val perm = MemberPermutation.build(this)
                memberPermutation = perm
                reorderedDims = ReorderedRoaring.build(this, perm)
            }
            if (experimental.jointProfile && jointProfile == null) {
                jointProfile = JointProfileIndex.build(this)
            }

            frozen = true
        }
    }

    /**
     * Read-safe snapshot of a single dim-value bitmap.
     * Frozen segment → returns the live (immutable) bitmap directly, no copy.
     * Open segment → clones under read-lock so concurrent add() cannot corrupt the view.
     */
    fun dimValueBitmap(
        dim: String,
        value: Int,
    ): RoaringBitmap? {
        val live = dims[dim]?.get(value) ?: return null
        if (frozen) return live
        return lock.read { synchronized(live) { live.clone() } }
    }

    fun dimValues(dim: String): Set<Int> = dims[dim]?.keys ?: emptySet()

    /**
     * Member universe: OR of every dim's bitmaps. On open segments, clones each bitmap
     * before folding so concurrent writes can't mutate mid-scan.
     */
    fun memberUniverse(): RoaringBitmap {
        frozenUniverse?.let { return it }
        return lock.read {
            val all = RoaringBitmap()
            dims.values.forEach { vm ->
                vm.values.forEach { bm ->
                    synchronized(bm) { all.or(bm) }
                }
            }
            all
        }
    }

    fun dimEncoder(dim: String): DimEncoder? = dimEncoders[dim]

    fun memberEncoder(): MemberEncoder = memberEncoder

    /**
     * Reordered counterpart of [dimValueBitmap]. Returns the bitmap in REORDERED-id space.
     * Caller is expected to translate back to canonical via [memberPermutation] if downstream
     * code expects canonical ids.
     */
    fun reorderedDimValueBitmap(
        dim: String,
        value: Int,
    ): RoaringBitmap? = reorderedDims?.get(dim)?.get(value)

    /**
     * OR of every reordered dim bitmap, cached. Returns null when no reordering exists.
     */
    fun reorderedMemberUniverse(): RoaringBitmap? {
        val rd = reorderedDims ?: return null
        reorderedUniverseCache?.let { return it }
        val all = RoaringBitmap()
        rd.values.forEach { vm -> vm.values.forEach { all.or(it) } }
        all.runOptimize()
        reorderedUniverseCache = all
        return all
    }

    /** IO layer hook: install a deserialized BSI for [dim]. Replaces any existing one. */
    fun attachBsi(
        dim: String,
        idx: BitSlicedIndex,
    ) {
        bsi[dim] = idx
    }

    /** IO layer hook: install a deserialized theta sample. */
    fun attachTheta(idx: ThetaSampleIndex) {
        theta = idx
    }

    /** IO layer hook: install a deserialized joint-profile index. */
    fun attachJointProfile(jp: JointProfileIndex) {
        jointProfile = jp
    }

    /**
     * IO layer hook: install a deserialized permutation and rebuild reordered dims from it.
     * Must be called AFTER canonical dims have been loaded.
     */
    fun attachMemberPermutation(perm: MemberPermutation) {
        memberPermutation = perm
        reorderedDims = ReorderedRoaring.build(this, perm)
    }

    sealed interface AddResult {
        data object Ok : AddResult

        data class Overflow(
            val dims: List<String>,
        ) : AddResult

        data object MemberMissing : AddResult

        data object RejectedFrozen : AddResult
    }

    companion object {
        fun create(
            id: Long,
            partition: Int,
            tStart: Instant,
            tEnd: Instant,
            cfg: IndexerConfig,
        ): Segment {
            val dimEncoders: Map<String, DimEncoder> = cfg.dimensions.associate { it.name to EncoderFactory.forDimension(it) }
            val bucketers: Map<String, Bucketer> =
                cfg.dimensions
                    .mapNotNull { d -> d.bucket?.let { d.name to BucketerFactory.create(it) } }
                    .toMap()
            val memberEncoder =
                MemberEncoder(
                    cfg.member,
                    if (cfg.member.encoding ==
                        io.conduktor.kri.config.Member.Encoding.DICT
                    ) {
                        Dict()
                    } else {
                        null
                    },
                )
            val metricFields = cfg.metrics.associate { it.name to it.field?.let { f -> cfg.fields[f]?.path?.let { _ -> f } } }
            return Segment(
                id = id,
                partition = partition,
                tStart = tStart,
                tEnd = tEnd,
                dimSpecs = cfg.dimensions,
                metricSpecs = cfg.metrics,
                bucketers = bucketers,
                memberEncoder = memberEncoder,
                dimEncoders = dimEncoders,
                memberFieldName = cfg.member.field,
                metricFields = metricFields,
                experimental = cfg.experimentalModels,
            )
        }
    }
}

package io.conduktor.kri.index

/**
 * Identifies an index model that can answer a query alongside the canonical Roaring path.
 * Parallel models coexist with the existing dims/ullSketches state — they never replace it.
 */
enum class IndexModelKind {
    /** The canonical path: per-(dim, value) Roaring bitmaps + ULL sketches. Always present. */
    ROARING,

    /** Bit-Sliced Index — exact range / sum over numeric dims without bucketing. */
    BSI,

    /** Theta sample on the member axis — distinct-under-filter that intersects correctly. */
    THETA,

    /** Joint-profile dictionary — AND-categorical filtering via shared cohort signatures. */
    JOINT_PROFILE,

    /** Roaring rebuilt over the reordered member axis — same semantics, different bytes/perf. */
    ROARING_REORDERED,
    ;

    companion object {
        fun parseList(raw: String?): List<IndexModelKind> {
            if (raw.isNullOrBlank()) return listOf(ROARING)
            return raw
                .split(',')
                .map { it.trim().uppercase() }
                .filter { it.isNotEmpty() }
                .mapNotNull { name -> entries.firstOrNull { it.name == name } }
                .ifEmpty { listOf(ROARING) }
        }
    }
}

/** One per-model outcome. value/approx are populated when supported=true. */
data class ModelResult(
    val model: IndexModelKind,
    val supported: Boolean,
    val value: Any?,
    val approx: Boolean,
    val timeNs: Long,
    val error: String? = null,
)

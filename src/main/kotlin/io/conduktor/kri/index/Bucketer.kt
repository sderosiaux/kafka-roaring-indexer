package io.conduktor.kri.index

import io.conduktor.kri.config.BucketSpec
import kotlin.math.ln
import kotlin.math.max

/** Maps a numeric value to a bucket uint32 id. Stable, deterministic. */
interface Bucketer {
    fun bucket(v: Double): Int
}

class LinearBucketer(
    spec: BucketSpec,
) : Bucketer {
    private val step = spec.step ?: error("linear bucket requires step")
    private val min = spec.min ?: 0.0
    private val max = spec.max ?: Double.MAX_VALUE

    override fun bucket(v: Double): Int {
        val clamped = v.coerceIn(min, max)
        return ((clamped - min) / step).toInt().coerceAtLeast(0)
    }
}

class ExponentialBucketer(
    spec: BucketSpec,
) : Bucketer {
    private val base = spec.base ?: error("exponential bucket requires base")
    private val min = max(spec.min ?: 1.0, 1e-300)
    private val max = spec.max ?: Double.MAX_VALUE
    private val lnBase = ln(base)

    override fun bucket(v: Double): Int {
        if (v <= min) return 0
        val clamped = if (v > max) max else v
        return (ln(clamped / min) / lnBase).toInt() + 1
    }
}

class ExplicitBucketer(
    spec: BucketSpec,
) : Bucketer {
    private val boundaries = (spec.boundaries ?: error("explicit bucket requires boundaries")).sorted().toDoubleArray()

    override fun bucket(v: Double): Int {
        var lo = 0
        var hi = boundaries.size
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (boundaries[mid] <= v) lo = mid + 1 else hi = mid
        }
        return lo
    }
}

object BucketerFactory {
    fun create(spec: BucketSpec): Bucketer =
        when (spec.type) {
            BucketSpec.Type.LINEAR -> LinearBucketer(spec)
            BucketSpec.Type.EXPONENTIAL -> ExponentialBucketer(spec)
            BucketSpec.Type.EXPLICIT -> ExplicitBucketer(spec)
        }
}

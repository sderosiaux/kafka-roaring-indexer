package io.conduktor.kri.index

import org.roaringbitmap.RoaringBitmap
import java.io.DataInputStream
import java.io.DataOutputStream

/**
 * Bit-Sliced Index over a numeric uint32 dimension.
 *
 * For each bit position b ∈ [0, bitWidth), `slices[b]` is a RoaringBitmap of memberIds whose
 * dimension value has bit b set. `existence` is a RoaringBitmap of memberIds that have a value
 * indexed (any record for that member).
 *
 * Idempotent under replay: setBit on the same memberId is a no-op for that record's value.
 * NOTE: BSI assumes one value per member per segment — the *latest* value wins on update.
 * For Kafka replay this is safe iff the same record always produces the same value (idempotent).
 *
 * Range / equality / aggregation primitives use O'Neil-style bit-by-bit comparisons producing a
 * RoaringBitmap of matching memberIds.
 */
class BitSlicedIndex(
    val bitWidth: Int = 32,
) {
    val slices: Array<RoaringBitmap> = Array(bitWidth) { RoaringBitmap() }
    val existence: RoaringBitmap = RoaringBitmap()

    /** Number of distinct memberIds indexed (cardinality of existence). */
    fun size(): Int = existence.cardinality

    /**
     * Index the value of memberId. If the member already had a different value, the old value's
     * bits are overwritten — last-write-wins. For replay safety, callers should ensure a record
     * always produces the same value for the same (member, segment).
     */
    fun set(
        memberId: Int,
        value: Long,
    ) {
        require(value in 0..0xFFFF_FFFFL) { "BSI value out of uint32 range: $value" }
        if (existence.contains(memberId)) {
            // Clear previous bits to allow update.
            for (b in 0 until bitWidth) {
                if (slices[b].contains(memberId)) slices[b].remove(memberId)
            }
        }
        existence.add(memberId)
        var v = value
        var b = 0
        while (v != 0L && b < bitWidth) {
            if ((v and 1L) != 0L) slices[b].add(memberId)
            v = v ushr 1
            b++
        }
    }

    fun runOptimize() {
        slices.forEach { it.runOptimize() }
        existence.runOptimize()
    }

    // ── Query primitives ───────────────────────────────────────────────────────

    /** Members whose value equals K, intersected with `restrict` if provided. */
    fun eq(
        k: Long,
        restrict: RoaringBitmap? = null,
    ): RoaringBitmap {
        var acc = restrict?.clone() ?: existence.clone()
        for (b in 0 until bitWidth) {
            val bitSet = ((k ushr b) and 1L) != 0L
            acc =
                if (bitSet) {
                    RoaringBitmap.and(acc, slices[b])
                } else {
                    RoaringBitmap.andNot(acc, slices[b])
                }
            if (acc.isEmpty) return acc
        }
        return acc
    }

    /** Members whose value is strictly less than K, intersected with `restrict` if provided. */
    fun lt(
        k: Long,
        restrict: RoaringBitmap? = null,
    ): RoaringBitmap {
        // O'Neil BSI compare: walk from top bit down.
        // result := ∅; eq := restrict (or existence)
        // For each bit b from high to low:
        //   K_b = bit b of K
        //   if K_b == 1: result |= eq AND_NOT slices[b];   eq := eq AND slices[b]
        //   else:        eq := eq AND_NOT slices[b]
        var result = RoaringBitmap()
        var eq = restrict?.clone() ?: existence.clone()
        for (b in bitWidth - 1 downTo 0) {
            val kb = ((k ushr b) and 1L) != 0L
            if (kb) {
                result = RoaringBitmap.or(result, RoaringBitmap.andNot(eq, slices[b]))
                eq = RoaringBitmap.and(eq, slices[b])
            } else {
                eq = RoaringBitmap.andNot(eq, slices[b])
            }
            if (eq.isEmpty && !kb) {
                // No equal candidates remain on a 0-bit; further iterations only constrain eq.
                // Continue to allow result to absorb further low-bit contributions on K=1 bits — but with eq empty those are all empty.
                break
            }
        }
        return result
    }

    fun le(
        k: Long,
        restrict: RoaringBitmap? = null,
    ): RoaringBitmap = if (k >= (1L shl bitWidth) - 1) restrict?.clone() ?: existence.clone() else lt(k + 1, restrict)

    fun gt(
        k: Long,
        restrict: RoaringBitmap? = null,
    ): RoaringBitmap {
        val base = restrict?.clone() ?: existence.clone()
        return RoaringBitmap.andNot(base, le(k, restrict))
    }

    fun ge(
        k: Long,
        restrict: RoaringBitmap? = null,
    ): RoaringBitmap {
        val base = restrict?.clone() ?: existence.clone()
        return RoaringBitmap.andNot(base, lt(k, restrict))
    }

    /** Members with value in [lo, hi] (inclusive), intersected with `restrict` if provided. */
    fun between(
        lo: Long,
        hi: Long,
        restrict: RoaringBitmap? = null,
    ): RoaringBitmap = RoaringBitmap.and(ge(lo, restrict), le(hi, restrict))

    /**
     * Sum of dim values for members in `mask`. Computed as Σ_b (2^b · |mask ∩ slices[b]|).
     * Avoids materializing per-member values.
     */
    fun sum(mask: RoaringBitmap): Long {
        var total = 0L
        for (b in 0 until bitWidth) {
            val c = RoaringBitmap.andCardinality(mask, slices[b]).toLong()
            total += c shl b
        }
        return total
    }

    // ── Serialization ──────────────────────────────────────────────────────────
    // Format: [int bitWidth][bitmap existence][bitmap slices[0]]..[bitmap slices[n-1]]
    //   each bitmap is [int sizeBytes][bytes...]

    fun serialize(out: DataOutputStream) {
        out.writeInt(bitWidth)
        writeBitmap(out, existence)
        slices.forEach { writeBitmap(out, it) }
    }

    companion object {
        fun deserialize(input: DataInputStream): BitSlicedIndex {
            val bw = input.readInt()
            val bsi = BitSlicedIndex(bw)
            readBitmapInto(input, bsi.existence)
            for (b in 0 until bw) readBitmapInto(input, bsi.slices[b])
            return bsi
        }

        private fun writeBitmap(
            out: DataOutputStream,
            bm: RoaringBitmap,
        ) {
            val buf = java.io.ByteArrayOutputStream()
            bm.serialize(DataOutputStream(buf))
            val bytes = buf.toByteArray()
            out.writeInt(bytes.size)
            out.write(bytes)
        }

        private fun readBitmapInto(
            input: DataInputStream,
            target: RoaringBitmap,
        ) {
            val n = input.readInt()
            val bytes = ByteArray(n)
            input.readFully(bytes)
            target.deserialize(DataInputStream(bytes.inputStream()))
        }
    }
}

package io.conduktor.kri.index

import com.dynatrace.hash4j.hashing.Hashing
import org.roaringbitmap.RoaringBitmap
import java.io.DataInputStream
import java.io.DataOutputStream
import java.util.concurrent.ConcurrentHashMap

/**
 * Theta sample on the member axis — a fixed fraction of memberIds is sampled and a parallel
 * dim-bitmap structure is built only over the sampled members.
 *
 * Why this matters: ULL/HLL sketches do NOT intersect correctly. A query like
 * `distinct(userId) WHERE country=FR AND device=mobile` cannot be answered exactly from
 * separate per-(country, device) ULLs. Theta samples support correct intersection: just
 * intersect the per-dim sample bitmaps and scale by 1/sampleRate.
 *
 * Inclusion: a record is sampled iff `hash64(memberId) < theta` (unsigned). Same rule across
 * segments → union of samples is consistent under replay.
 *
 * Estimate: `cardinality(filtered_sample) / sampleRate`.
 *  - Standard error ~ 1 / sqrt(N · sampleRate). With sampleRate = 1/16, ~25% relative error
 *    at N = 100; ~0.8% at N = 100k.
 */
class ThetaSampleIndex(
    /** Inclusion threshold (unsigned). Members with hash64(id) < theta are kept. */
    val theta: Long,
) {
    /** dim → (valueId → RoaringBitmap of *sampled* memberIds). Same shape as Segment.dims. */
    val dims: MutableMap<String, MutableMap<Int, RoaringBitmap>> = ConcurrentHashMap()

    /** All sampled memberIds in this segment. Used as universe for NOT operations. */
    val sampledUniverse: RoaringBitmap = RoaringBitmap()

    val sampleRate: Double
        get() {
            // theta is an unsigned long compared against hash64; sampleRate = theta / 2^64.
            // For theta ≤ Long.MAX_VALUE we can compute directly; otherwise scale.
            return if (theta >= 0) {
                theta.toDouble() / TWO_POW_64
            } else {
                // theta > Long.MAX_VALUE: split into upper + lower
                (theta.toULong().toDouble()) / TWO_POW_64
            }
        }

    /** Returns true iff the record was included in the sample. */
    fun maybeAdd(
        memberId: Int,
        dimValuePairs: List<Pair<String, Int>>,
    ): Boolean {
        val h = hashMember(memberId)
        if (java.lang.Long.compareUnsigned(h, theta) >= 0) return false
        sampledUniverse.add(memberId)
        for ((dim, valueId) in dimValuePairs) {
            val valueMap = dims.computeIfAbsent(dim) { ConcurrentHashMap() }
            val bm = synchronized(valueMap) { valueMap.getOrPut(valueId) { RoaringBitmap() } }
            synchronized(bm) { bm.add(memberId) }
        }
        return true
    }

    fun runOptimize() {
        dims.values.forEach { vm -> vm.values.forEach { it.runOptimize() } }
        sampledUniverse.runOptimize()
    }

    fun dimValueBitmap(
        dim: String,
        value: Int,
    ): RoaringBitmap? = dims[dim]?.get(value)

    // ── Serialization ──────────────────────────────────────────────────────────
    // Format: [long theta][int nDims] then per-dim:
    //   [utf dimName][int nValues] then per-value: [int valueId][int sizeBytes][bytes...]
    // [int sizeBytes][bytes...]   (sampledUniverse)

    fun serialize(out: DataOutputStream) {
        out.writeLong(theta)
        out.writeInt(dims.size)
        dims.forEach { (dim, valueMap) ->
            out.writeUTF(dim)
            out.writeInt(valueMap.size)
            valueMap.forEach { (valueId, bm) ->
                out.writeInt(valueId)
                writeBitmap(out, bm)
            }
        }
        writeBitmap(out, sampledUniverse)
    }

    companion object {
        private const val TWO_POW_64: Double = 18446744073709551616.0 // 2^64

        fun hashMember(memberId: Int): Long =
            Hashing
                .komihash5_0()
                .hashBytesToLong(byteArrayOfMember(memberId))

        private fun byteArrayOfMember(id: Int): ByteArray {
            // Stable LE encoding so the same uint32 always hashes the same way.
            val b = ByteArray(4)
            b[0] = (id and 0xFF).toByte()
            b[1] = ((id ushr 8) and 0xFF).toByte()
            b[2] = ((id ushr 16) and 0xFF).toByte()
            b[3] = ((id ushr 24) and 0xFF).toByte()
            return b
        }

        fun thetaFor(sampleRate: Double): Long {
            require(sampleRate in 0.0..1.0) { "sampleRate must be in [0,1]" }
            // Map (0, 1] → (0, 2^64]. Use ULong arithmetic to avoid Long overflow.
            val raw = (sampleRate * TWO_POW_64)
            return if (raw >= TWO_POW_64) -1L else raw.toLong()
        }

        fun deserialize(input: DataInputStream): ThetaSampleIndex {
            val theta = input.readLong()
            val idx = ThetaSampleIndex(theta)
            val nDims = input.readInt()
            repeat(nDims) {
                val dim = input.readUTF()
                val n = input.readInt()
                val map = idx.dims.computeIfAbsent(dim) { ConcurrentHashMap() }
                repeat(n) {
                    val valueId = input.readInt()
                    val bm = readBitmap(input)
                    map[valueId] = bm
                }
            }
            readBitmapInto(input, idx.sampledUniverse)
            return idx
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

        private fun readBitmap(input: DataInputStream): RoaringBitmap {
            val n = input.readInt()
            val bytes = ByteArray(n)
            input.readFully(bytes)
            val bm = RoaringBitmap()
            bm.deserialize(DataInputStream(bytes.inputStream()))
            return bm
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

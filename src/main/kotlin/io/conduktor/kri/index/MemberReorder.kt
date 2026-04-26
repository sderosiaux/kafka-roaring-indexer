package io.conduktor.kri.index

import org.roaringbitmap.RoaringBitmap
import java.io.DataInputStream
import java.io.DataOutputStream
import java.util.concurrent.ConcurrentHashMap

/**
 * Permutation of segment-local memberIds, computed at freeze.
 *
 * Goal: cluster members with the same / similar atom signatures into contiguous newId ranges so
 * Roaring run-containers can absorb long runs and dim bitmaps shrink dramatically. Same idea as
 * "reorder by sort key" in column stores.
 *
 * The permutation is materialized as two maps; downstream models (BSI, joint-profile, reordered
 * Roaring) translate memberIds via [oldToNew] when adding bits.
 */
class MemberPermutation(
    /** Old memberId → new memberId. Members not in the map keep their old id (shouldn't happen in well-formed segments). */
    val oldToNew: Map<Int, Int>,
    /** new memberId → old memberId. Inverse of oldToNew, indexed for O(1) translation. */
    val newToOld: IntArray,
) {
    fun translate(oldId: Int): Int = oldToNew[oldId] ?: oldId

    fun size(): Int = oldToNew.size

    /** Translate a bitmap of CANONICAL ids into REORDERED-id space. */
    fun translateForward(canonical: RoaringBitmap): RoaringBitmap {
        val out = RoaringBitmap()
        val it = canonical.intIterator
        while (it.hasNext()) {
            val oldId = it.next()
            oldToNew[oldId]?.let { out.add(it) }
        }
        return out
    }

    /** Translate a bitmap of REORDERED ids back into CANONICAL-id space. */
    fun translateBack(reordered: RoaringBitmap): RoaringBitmap {
        val out = RoaringBitmap()
        val it = reordered.intIterator
        while (it.hasNext()) {
            val newId = it.next()
            if (newId in newToOld.indices) out.add(newToOld[newId])
        }
        return out
    }

    companion object {
        /**
         * Build the permutation from a frozen segment by sorting memberIds lexicographically by
         * their sorted atom-set signature (atomId list).
         *
         * Members with the same atom set are grouped together; that's the same property the
         * joint-profile dictionary exploits, so reordering composes naturally with it.
         */
        fun build(seg: Segment): MemberPermutation {
            val atomDict = HashMap<Pair<String, Int>, Int>()
            seg.dims.forEach { (dim, valueMap) ->
                valueMap.keys.sorted().forEach { vid ->
                    val key = dim to vid
                    atomDict.getOrPut(key) { atomDict.size }
                }
            }

            // (member → sorted atom signature)
            val memberSig = HashMap<Int, IntArray>()
            seg.dims.forEach { (dim, valueMap) ->
                valueMap.forEach { (vid, bm) ->
                    val atomId = atomDict.getValue(dim to vid)
                    val it = bm.intIterator
                    while (it.hasNext()) {
                        val m = it.next()
                        val cur = memberSig[m]
                        memberSig[m] =
                            if (cur == null) {
                                intArrayOf(atomId)
                            } else {
                                // Append + sort (small lists; fine).
                                val newArr = IntArray(cur.size + 1)
                                System.arraycopy(cur, 0, newArr, 0, cur.size)
                                newArr[cur.size] = atomId
                                newArr.sort()
                                newArr
                            }
                    }
                }
            }

            // Sort members by signature lexicographically.
            val sorted =
                memberSig.entries
                    .sortedWith(
                        Comparator { a, b ->
                            val sa = a.value
                            val sb = b.value
                            val n = minOf(sa.size, sb.size)
                            for (i in 0 until n) {
                                val cmp = sa[i].compareTo(sb[i])
                                if (cmp != 0) return@Comparator cmp
                            }
                            sa.size.compareTo(sb.size)
                        },
                    )

            val oldToNew = HashMap<Int, Int>(sorted.size)
            val newToOld = IntArray(sorted.size)
            sorted.forEachIndexed { newId, entry ->
                oldToNew[entry.key] = newId
                newToOld[newId] = entry.key
            }
            return MemberPermutation(oldToNew, newToOld)
        }

        fun serialize(
            perm: MemberPermutation,
            out: DataOutputStream,
        ) {
            out.writeInt(perm.newToOld.size)
            perm.newToOld.forEach { out.writeInt(it) }
        }

        fun deserialize(input: DataInputStream): MemberPermutation {
            val n = input.readInt()
            val newToOld = IntArray(n) { input.readInt() }
            val oldToNew = HashMap<Int, Int>(n)
            for (i in 0 until n) oldToNew[newToOld[i]] = i
            return MemberPermutation(oldToNew, newToOld)
        }
    }
}

/**
 * Reordered Roaring dims: same shape as Segment.dims but with memberIds remapped through a
 * MemberPermutation. Built at freeze when the reorderMembers experiment is enabled.
 *
 * Query path: identical to canonical Roaring — the Evaluator just reads from these bitmaps
 * instead of the original. Result cardinality is preserved (permutation is a bijection); only
 * bytes-per-bitmap and run-container density change.
 */
object ReorderedRoaring {
    fun build(
        seg: Segment,
        perm: MemberPermutation,
    ): MutableMap<String, MutableMap<Int, RoaringBitmap>> {
        val out: MutableMap<String, MutableMap<Int, RoaringBitmap>> = ConcurrentHashMap()
        seg.dims.forEach { (dim, valueMap) ->
            val target = ConcurrentHashMap<Int, RoaringBitmap>()
            valueMap.forEach { (vid, bm) ->
                val rebuilt = RoaringBitmap()
                val it = bm.intIterator
                while (it.hasNext()) {
                    rebuilt.add(perm.translate(it.next()))
                }
                rebuilt.runOptimize()
                target[vid] = rebuilt
            }
            out[dim] = target
        }
        return out
    }
}

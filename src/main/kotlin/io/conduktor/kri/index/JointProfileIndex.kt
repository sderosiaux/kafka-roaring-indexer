package io.conduktor.kri.index

import org.roaringbitmap.RoaringBitmap
import java.io.DataInputStream
import java.io.DataOutputStream

/**
 * Joint-profile dictionary built at segment freeze.
 *
 * For each member, we collect its set of (dim, value) atoms across all records in the segment
 * and hash that set into a profileId. Members with the same atom set share a profile. K profiles
 * where K ≪ #members when users cluster into cohorts (typical for product analytics).
 *
 * Query path for a categorical AND/OR/NOT filter: filter happens in profile space first
 * (much smaller cardinality than member space) and the matching profiles are then expanded into
 * a member bitmap. Range predicates are NOT supported by this model — eval should fall back.
 *
 * Memory win: profileToMembers stores member groups densely; atomToProfiles is small (K bits per
 * atom instead of N). When profile cardinality is low (K/N < 0.1), big saves; when each member
 * has its own profile (K = N), this model degenerates and should not be enabled.
 */
class JointProfileIndex(
    /** profileId → list of atomIds composing the profile (sorted, deduped). */
    val profileToAtoms: Map<Int, IntArray>,
    /** profileId → RoaringBitmap of memberIds in this profile. */
    val profileToMembers: Map<Int, RoaringBitmap>,
    /** atomId → RoaringBitmap of profileIds containing this atom. */
    val atomToProfiles: Map<Int, RoaringBitmap>,
    /** (dim, valueId) → atomId. The atom dictionary, segment-local. */
    val atomDict: Map<DimValue, Int>,
    /** atomId → (dim, valueId), inverse for serialization / debugging. */
    val atomReverse: Array<DimValue>,
    /** RoaringBitmap of every profileId — universe for NOT. */
    val profileUniverse: RoaringBitmap,
) {
    data class DimValue(
        val dim: String,
        val valueId: Int,
    )

    fun profileCount(): Int = profileToAtoms.size

    fun atomCount(): Int = atomDict.size

    /** Returns the atomId for (dim, valueId), or null if no member ever had that atom in this segment. */
    fun atomFor(
        dim: String,
        valueId: Int,
    ): Int? = atomDict[DimValue(dim, valueId)]

    /** Convert a profile bitmap into a member bitmap by OR'ing per-profile member groups. */
    fun expandProfilesToMembers(profiles: RoaringBitmap): RoaringBitmap {
        val out = RoaringBitmap()
        val it = profiles.intIterator
        while (it.hasNext()) {
            val pid = it.next()
            profileToMembers[pid]?.let { out.or(it) }
        }
        return out
    }

    fun runOptimize() {
        profileToMembers.values.forEach { it.runOptimize() }
        atomToProfiles.values.forEach { it.runOptimize() }
        profileUniverse.runOptimize()
    }

    // ── Serialization ──────────────────────────────────────────────────────────
    // Format:
    //   [int nAtoms] then per atom: [utf dim][int valueId]
    //   [int nProfiles] then per profile:
    //     [int profileId][int nAtomsInProfile] (atomIds...)
    //     [int sizeBytes][bytes...]   (members bitmap)
    //   [int nAtoms] then per atom: [int atomId][int sizeBytes][bytes...]   (profiles bitmap)
    //   [int sizeBytes][bytes...]   (profileUniverse)

    fun serialize(out: DataOutputStream) {
        out.writeInt(atomReverse.size)
        atomReverse.forEach {
            out.writeUTF(it.dim)
            out.writeInt(it.valueId)
        }
        out.writeInt(profileToAtoms.size)
        profileToAtoms.forEach { (pid, atoms) ->
            out.writeInt(pid)
            out.writeInt(atoms.size)
            atoms.forEach { out.writeInt(it) }
            val members = profileToMembers[pid] ?: RoaringBitmap()
            writeBitmap(out, members)
        }
        out.writeInt(atomToProfiles.size)
        atomToProfiles.forEach { (atomId, profiles) ->
            out.writeInt(atomId)
            writeBitmap(out, profiles)
        }
        writeBitmap(out, profileUniverse)
    }

    companion object {
        /**
         * Build a JointProfileIndex from a frozen segment's canonical Roaring dim bitmaps.
         * Intended to run at Segment.freeze() *after* runOptimize().
         */
        fun build(seg: Segment): JointProfileIndex {
            // Step 1: assign atomIds to every observed (dim, valueId) pair.
            val atomDict = HashMap<DimValue, Int>()
            val atomReverseList = ArrayList<DimValue>()
            seg.dims.forEach { (dim, valueMap) ->
                valueMap.keys.sorted().forEach { vid ->
                    val key = DimValue(dim, vid)
                    if (key !in atomDict) {
                        atomDict[key] = atomReverseList.size
                        atomReverseList.add(key)
                    }
                }
            }
            val atomReverse = atomReverseList.toTypedArray()

            // Step 2: invert (atom → members) into (member → atomIds).
            // Walk every (dim, value, bitmap), tag each member with the atomId.
            val memberToAtoms = HashMap<Int, MutableList<Int>>()
            seg.dims.forEach { (dim, valueMap) ->
                valueMap.forEach { (vid, bm) ->
                    val atomId = atomDict.getValue(DimValue(dim, vid))
                    val it = bm.intIterator
                    while (it.hasNext()) {
                        val m = it.next()
                        memberToAtoms.getOrPut(m) { ArrayList(8) }.add(atomId)
                    }
                }
            }

            // Step 3: hash sorted-atom-set per member → profileId.
            val signatureToPid = HashMap<List<Int>, Int>()
            val profileToAtoms = HashMap<Int, IntArray>()
            val profileToMembersBuilder = HashMap<Int, RoaringBitmap>()
            memberToAtoms.forEach { (memberId, atomList) ->
                val sorted = atomList.distinct().sorted()
                val pid =
                    signatureToPid.getOrPut(sorted) {
                        val newPid = signatureToPid.size
                        profileToAtoms[newPid] = sorted.toIntArray()
                        newPid
                    }
                profileToMembersBuilder.getOrPut(pid) { RoaringBitmap() }.add(memberId)
            }

            // Step 4: invert (profile → atoms) into (atom → profiles).
            val atomToProfilesBuilder = HashMap<Int, RoaringBitmap>()
            profileToAtoms.forEach { (pid, atoms) ->
                atoms.forEach { atomId ->
                    atomToProfilesBuilder.getOrPut(atomId) { RoaringBitmap() }.add(pid)
                }
            }

            val profileUniverse = RoaringBitmap()
            profileToAtoms.keys.forEach { profileUniverse.add(it) }

            // runOptimize after build.
            profileToMembersBuilder.values.forEach { it.runOptimize() }
            atomToProfilesBuilder.values.forEach { it.runOptimize() }
            profileUniverse.runOptimize()

            return JointProfileIndex(
                profileToAtoms = profileToAtoms,
                profileToMembers = profileToMembersBuilder,
                atomToProfiles = atomToProfilesBuilder,
                atomDict = atomDict,
                atomReverse = atomReverse,
                profileUniverse = profileUniverse,
            )
        }

        fun deserialize(input: DataInputStream): JointProfileIndex {
            val nAtoms = input.readInt()
            val atomReverse = Array(nAtoms) { DimValue(input.readUTF(), input.readInt()) }
            val atomDict = HashMap<DimValue, Int>().apply { atomReverse.forEachIndexed { i, dv -> put(dv, i) } }

            val nProfiles = input.readInt()
            val profileToAtoms = HashMap<Int, IntArray>()
            val profileToMembers = HashMap<Int, RoaringBitmap>()
            repeat(nProfiles) {
                val pid = input.readInt()
                val n = input.readInt()
                profileToAtoms[pid] = IntArray(n) { input.readInt() }
                profileToMembers[pid] = readBitmap(input)
            }

            val nA2 = input.readInt()
            val atomToProfiles = HashMap<Int, RoaringBitmap>()
            repeat(nA2) {
                val atomId = input.readInt()
                atomToProfiles[atomId] = readBitmap(input)
            }

            val universe = readBitmap(input)
            return JointProfileIndex(profileToAtoms, profileToMembers, atomToProfiles, atomDict, atomReverse, universe)
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
    }
}

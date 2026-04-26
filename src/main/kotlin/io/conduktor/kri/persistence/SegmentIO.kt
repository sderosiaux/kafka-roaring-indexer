package io.conduktor.kri.persistence

import com.dynatrace.hash4j.distinctcount.UltraLogLog
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.ZstdOutputStream
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.Member
import io.conduktor.kri.config.Metric
import io.conduktor.kri.config.Storage
import io.conduktor.kri.index.BitSlicedIndex
import io.conduktor.kri.index.DictEncoder
import io.conduktor.kri.index.JointProfileIndex
import io.conduktor.kri.index.MemberPermutation
import io.conduktor.kri.index.Segment
import io.conduktor.kri.index.ThetaSampleIndex
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/**
 * Persistence layout (per segment):
 *   {root}/segments/seg_{id}_{tStartMs}_{tEndMs}/
 *     meta.json
 *     dims/{dim}.rbi       portable Roaring, length-prefixed per value
 *     dicts/{dim}.dict     tab-separated "value<TAB>id" lines
 *     metrics/{name}.ull   length-prefixed sketches, one per slice
 *     metrics/{name}.count binary long counter
 *     metrics/{name}.sum   binary long counter
 */
class SegmentIO(
    private val cfg: IndexerConfig,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val json = ObjectMapper().registerKotlinModule()
    private val root: Path = Path.of(cfg.storage.path)
    private val segmentsDir: Path = root.resolve("segments")
    val manifest: SegmentManifest = SegmentManifest(root)

    init {
        Files.createDirectories(segmentsDir)
    }

    fun write(seg: Segment) {
        val dn = dirName(seg)
        val dir = segmentsDir.resolve(dn)
        val tmp = segmentsDir.resolve("$dn.tmp")
        if (Files.exists(tmp)) deleteRecursively(tmp)
        Files.createDirectories(tmp.resolve("dims"))
        Files.createDirectories(tmp.resolve("dicts"))
        Files.createDirectories(tmp.resolve("metrics"))
        Files.createDirectories(tmp.resolve("models"))

        writeMeta(seg, tmp.resolve("meta.json"))
        writeDims(seg, tmp.resolve("dims"))
        writeDicts(seg, tmp.resolve("dicts"))
        writeMetrics(seg, tmp.resolve("metrics"))
        writeExperimentalModels(seg, tmp.resolve("models"))

        fsyncTree(tmp)
        if (Files.exists(dir)) deleteRecursively(dir)
        Files.move(tmp, dir, StandardCopyOption.ATOMIC_MOVE)
        manifest.upsert(SegmentManifest.segmentToEntry(seg, dn))
        log.info("Persisted segment {} partition={} to {}", seg.id, seg.partition, dir)
    }

    fun loadAll(
        store: io.conduktor.kri.index.SegmentStore,
        nextId: AtomicLong,
    ) {
        val entries = manifest.load()
        if (entries.isNotEmpty()) {
            entries.forEach { e ->
                val dir = segmentsDir.resolve(e.dir)
                runCatching {
                    val seg = read(dir)
                    store.registerFrozen(seg)
                    if (seg.id >= nextId.get()) nextId.set(seg.id + 1)
                    log.info(
                        "Loaded segment {} partition={} records={} [{} .. {}) (via manifest)",
                        seg.id,
                        seg.partition,
                        seg.recordCount.get(),
                        seg.tStart,
                        seg.tEnd,
                    )
                }.onFailure { log.warn("manifest entry {} unreadable: {}", dir, it.message) }
            }
            return
        }
        // Fallback: scan directory (legacy / manifest missing).
        if (!Files.exists(segmentsDir)) return
        val rebuilt = mutableListOf<SegmentManifest.Entry>()
        Files.list(segmentsDir).use { stream ->
            stream
                .filter { Files.isDirectory(it) && !it.fileName.toString().endsWith(".tmp") }
                .sorted()
                .forEach { dir ->
                    runCatching {
                        val seg = read(dir)
                        store.registerFrozen(seg)
                        if (seg.id >= nextId.get()) nextId.set(seg.id + 1)
                        rebuilt.add(SegmentManifest.segmentToEntry(seg, dir.fileName.toString()))
                        log.info(
                            "Loaded segment {} partition={} records={} [{} .. {}) (via scan)",
                            seg.id,
                            seg.partition,
                            seg.recordCount.get(),
                            seg.tStart,
                            seg.tEnd,
                        )
                    }.onFailure { log.warn("skip segment at {}: {}", dir, it.message) }
                }
        }
        if (rebuilt.isNotEmpty()) manifest.save(rebuilt)
    }

    private fun dirName(seg: Segment): String =
        "seg_${"%020d".format(seg.id)}_p${"%05d".format(seg.partition)}_${seg.tStart.toEpochMilli()}_${seg.tEnd.toEpochMilli()}"

    private fun writeMeta(
        seg: Segment,
        file: Path,
    ) {
        val meta =
            mapOf(
                "id" to seg.id,
                "partition" to seg.partition,
                "tStartMs" to seg.tStart.toEpochMilli(),
                "tEndMs" to seg.tEnd.toEpochMilli(),
                "recordCount" to seg.recordCount.get(),
                "offsets" to seg.offsets.mapValues { mapOf("first" to it.value.first, "last" to it.value.last) },
                "schemaVersion" to 1,
                "dimNames" to cfg.dimensions.map { it.name },
                "metricNames" to cfg.metrics.map { it.name },
            )
        Files.newBufferedWriter(file).use { it.write(json.writerWithDefaultPrettyPrinter().writeValueAsString(meta)) }
    }

    private fun writeDims(
        seg: Segment,
        dir: Path,
    ) {
        seg.dims.forEach { (dim, valueMap) ->
            DataOutputStream(openOutputStream(dir.resolve("$dim.rbi"))).use { out ->
                out.writeInt(valueMap.size)
                valueMap.forEach { (valueId, bm) ->
                    out.writeInt(valueId)
                    val serialized = java.io.ByteArrayOutputStream()
                    bm.serialize(DataOutputStream(serialized))
                    val bytes = serialized.toByteArray()
                    out.writeInt(bytes.size)
                    out.write(bytes)
                }
                out.flush()
            }
        }
    }

    private fun writeDicts(
        seg: Segment,
        dir: Path,
    ) {
        fun writeDict(
            path: Path,
            entries: Sequence<Pair<String, Int>>,
        ) {
            openOutputStream(path).bufferedWriter().use { w ->
                entries.forEach { (value, id) ->
                    w.write(id.toString())
                    w.write('\t'.code)
                    w.write(value.replace("\n", "\\n").replace("\t", "\\t"))
                    w.write('\n'.code)
                }
            }
        }
        cfg.dimensions.forEach { d ->
            val enc = seg.dimEncoder(d.name)
            if (enc is DictEncoder) {
                writeDict(dir.resolve("${d.name}.dict"), enc.dict().entries().asSequence())
            }
        }
        if (cfg.member.encoding == Member.Encoding.DICT) {
            seg.memberEncoder().dict()?.let { md ->
                writeDict(dir.resolve("_member.dict"), md.entries().asSequence())
            }
        }
    }

    private fun writeExperimentalModels(
        seg: Segment,
        dir: Path,
    ) {
        // BSI per opted-in dim → models/{dim}.bsi
        seg.bsi.forEach { (dim, idx) ->
            DataOutputStream(openOutputStream(dir.resolve("$dim.bsi"))).use { out -> idx.serialize(out) }
        }
        // Theta sample → models/theta.sample (single per-segment file)
        seg.theta?.let { th ->
            DataOutputStream(openOutputStream(dir.resolve("theta.sample"))).use { out -> th.serialize(out) }
        }
        // Joint-profile → models/joint.profile
        seg.jointProfile?.let { jp ->
            DataOutputStream(openOutputStream(dir.resolve("joint.profile"))).use { out -> jp.serialize(out) }
        }
        // Member permutation → models/member.perm
        seg.memberPermutation?.let { perm ->
            DataOutputStream(openOutputStream(dir.resolve("member.perm"))).use { out ->
                MemberPermutation.serialize(perm, out)
            }
        }
        // Reordered dims are derived from canonical dims + permutation; no need to persist.
        // (Recomputed at load time when permutation is present.)
    }

    private fun writeMetrics(
        seg: Segment,
        dir: Path,
    ) {
        seg.ullSketches.forEach { (name, sliceMap) ->
            DataOutputStream(openOutputStream(dir.resolve("$name.ull"))).use { out ->
                out.writeInt(sliceMap.size)
                sliceMap.forEach { (slice, sk) ->
                    out.writeInt(slice.size)
                    slice.forEach { out.writeInt(it) }
                    val bytes = sk.state
                    out.writeInt(bytes.size)
                    out.write(bytes)
                }
            }
        }
        // Counters and sums are 8 bytes — not worth compressing.
        seg.counters.forEach { (name, v) ->
            DataOutputStream(FileOutputStream(dir.resolve("$name.count").toFile())).use { it.writeLong(v.get()) }
        }
        seg.sums.forEach { (name, v) ->
            DataOutputStream(FileOutputStream(dir.resolve("$name.sum").toFile())).use { it.writeLong(v.get()) }
        }
    }

    /** Wraps with ZstdOutputStream when compression is enabled. */
    private fun openOutputStream(path: Path): OutputStream {
        val raw = FileOutputStream(path.toFile()).buffered()
        return if (cfg.storage.compress == Storage.Compress.ZSTD) ZstdOutputStream(raw, 3) else raw
    }

    /**
     * Auto-detects zstd via magic bytes (0x28 0xB5 0x2F 0xFD).
     * Backward-compatible: uncompressed segments are read as-is.
     */
    private fun openInputStream(path: Path): InputStream {
        val raw = FileInputStream(path.toFile()).buffered()
        raw.mark(4)
        val header = raw.readNBytes(4)
        raw.reset()
        return if (header.size == 4 &&
            header[0] == 0x28.toByte() &&
            header[1] == 0xB5.toByte() &&
            header[2] == 0x2F.toByte() &&
            header[3] == 0xFD.toByte()
        ) {
            ZstdInputStream(raw)
        } else {
            raw
        }
    }

    fun read(dir: Path): Segment {
        val meta = json.readTree(Files.readAllBytes(dir.resolve("meta.json")))
        val id = meta.get("id").asLong()
        val partition = meta.get("partition")?.asInt() ?: 0
        val tStart = Instant.ofEpochMilli(meta.get("tStartMs").asLong())
        val tEnd = Instant.ofEpochMilli(meta.get("tEndMs").asLong())
        val seg = Segment.create(id, partition, tStart, tEnd, cfg)

        // Restore dicts BEFORE dims so dict encoders know their ids.
        val dictsDir = dir.resolve("dicts")
        cfg.dimensions.forEach { d ->
            val f = dictsDir.resolve("${d.name}.dict")
            if (Files.exists(f)) {
                val enc = seg.dimEncoder(d.name)
                if (enc is DictEncoder) {
                    openInputStream(f).bufferedReader().useLines { lines ->
                        lines.forEach { line ->
                            val parts = line.split('\t', limit = 2)
                            if (parts.size == 2) enc.dict().getOrIntern(parts[1].replace("\\n", "\n"), null)
                        }
                    }
                }
            }
        }

        // Dims
        val dimsDir = dir.resolve("dims")
        cfg.dimensions.forEach { d ->
            val f = dimsDir.resolve("${d.name}.rbi")
            if (Files.exists(f)) {
                DataInputStream(openInputStream(f)).use { inp ->
                    val n = inp.readInt()
                    val map = seg.dims.getValue(d.name)
                    repeat(n) {
                        val valueId = inp.readInt()
                        val size = inp.readInt()
                        val bytes = ByteArray(size)
                        inp.readFully(bytes)
                        val bm = RoaringBitmap()
                        bm.deserialize(DataInputStream(bytes.inputStream()))
                        map[valueId] = bm
                    }
                }
            }
        }

        // Metrics
        val metricsDir = dir.resolve("metrics")
        cfg.metrics.forEach { m ->
            when (m.type) {
                Metric.Type.ULL -> {
                    val f = metricsDir.resolve("${m.name}.ull")
                    if (Files.exists(f)) {
                        DataInputStream(openInputStream(f)).use { inp ->
                            val n = inp.readInt()
                            val map = seg.ullSketches.getValue(m.name)
                            repeat(n) {
                                val sliceLen = inp.readInt()
                                val slice = List(sliceLen) { inp.readInt() }
                                val size = inp.readInt()
                                val bytes = ByteArray(size)
                                inp.readFully(bytes)
                                map[slice] = UltraLogLog.wrap(bytes)
                            }
                        }
                    }
                }
                Metric.Type.COUNT -> {
                    val f = metricsDir.resolve("${m.name}.count")
                    if (Files.exists(f)) {
                        DataInputStream(FileInputStream(f.toFile())).use { inp ->
                            seg.counters[m.name]?.set(inp.readLong())
                        }
                    }
                }
                Metric.Type.SUM -> {
                    val f = metricsDir.resolve("${m.name}.sum")
                    if (Files.exists(f)) {
                        DataInputStream(FileInputStream(f.toFile())).use { inp ->
                            seg.sums[m.name]?.set(inp.readLong())
                        }
                    }
                }
                else -> {}
            }
        }

        // Record count + offsets
        seg.recordCount.set(meta.get("recordCount").asLong())
        meta.get("offsets")?.fields()?.forEach { (pStr, range) ->
            val p = pStr.toInt()
            val first = range.get("first").asLong()
            val last = range.get("last").asLong()
            seg.offsets[p] = first..last
        }

        // Experimental models: load from models/ directory if present.
        // freeze() will rebuild reorderedDims from the loaded permutation, so we attach
        // permutation BEFORE freeze() to short-circuit the freeze-time recomputation.
        loadExperimentalModels(seg, dir.resolve("models"))

        seg.freeze()
        return seg
    }

    private fun loadExperimentalModels(
        seg: Segment,
        dir: Path,
    ) {
        if (!Files.exists(dir)) return
        cfg.experimentalModels.bsiDims.forEach { dim ->
            val f = dir.resolve("$dim.bsi")
            if (Files.exists(f)) {
                DataInputStream(openInputStream(f)).use { inp ->
                    seg.attachBsi(dim, BitSlicedIndex.deserialize(inp))
                }
            }
        }
        if (cfg.experimentalModels.thetaSample) {
            val f = dir.resolve("theta.sample")
            if (Files.exists(f)) {
                DataInputStream(openInputStream(f)).use { inp ->
                    seg.attachTheta(ThetaSampleIndex.deserialize(inp))
                }
            }
        }
        if (cfg.experimentalModels.jointProfile) {
            val f = dir.resolve("joint.profile")
            if (Files.exists(f)) {
                DataInputStream(openInputStream(f)).use { inp ->
                    seg.attachJointProfile(JointProfileIndex.deserialize(inp))
                }
            }
        }
        if (cfg.experimentalModels.reorderMembers) {
            val f = dir.resolve("member.perm")
            if (Files.exists(f)) {
                DataInputStream(openInputStream(f)).use { inp ->
                    seg.attachMemberPermutation(MemberPermutation.deserialize(inp))
                }
            }
        }
    }

    private fun fsyncTree(dir: Path) {
        Files.walk(dir).use { stream ->
            stream.filter { Files.isRegularFile(it) }.forEach { f ->
                runCatching {
                    java.nio.channels.FileChannel.open(f, java.nio.file.StandardOpenOption.WRITE).use { ch ->
                        ch.force(true)
                    }
                }
            }
        }
    }

    fun gcOldSegments(
        retentionMs: Long,
        store: io.conduktor.kri.index.SegmentStore,
    ) {
        val cutoffMs = System.currentTimeMillis() - retentionMs
        val current = manifest.load()
        val (expired, keep) = current.partition { it.endMs < cutoffMs }
        if (expired.isEmpty()) return
        expired.forEach { e ->
            val dir = segmentsDir.resolve(e.dir)
            runCatching { deleteRecursively(dir) }
                .onSuccess { log.info("GC: deleted segment dir {}", dir) }
                .onFailure { log.warn("GC: failed to delete {}: {}", dir, it.message) }
            store.unregisterFrozen(e.id)
        }
        manifest.save(keep)
        log.info("GC: removed {} expired segment(s), {} remain", expired.size, keep.size)
    }

    private fun deleteRecursively(p: Path) {
        if (!Files.exists(p)) return
        Files.walk(p).use { s ->
            s.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
        }
    }
}

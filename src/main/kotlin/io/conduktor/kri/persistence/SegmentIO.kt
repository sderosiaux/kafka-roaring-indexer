package io.conduktor.kri.persistence

import com.dynatrace.hash4j.distinctcount.UltraLogLog
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.Member
import io.conduktor.kri.config.Metric
import io.conduktor.kri.index.DictEncoder
import io.conduktor.kri.index.Segment
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.FileInputStream
import java.io.FileOutputStream
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

        writeMeta(seg, tmp.resolve("meta.json"))
        writeDims(seg, tmp.resolve("dims"))
        writeDicts(seg, tmp.resolve("dicts"))
        writeMetrics(seg, tmp.resolve("metrics"))

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
            val f = dir.resolve("$dim.rbi").toFile()
            DataOutputStream(FileOutputStream(f).buffered()).use { out ->
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
        cfg.dimensions.forEach { d ->
            val enc = seg.dimEncoder(d.name)
            if (enc is DictEncoder) {
                val dict = enc.dict()
                Files.newBufferedWriter(dir.resolve("${d.name}.dict")).use { w ->
                    dict.entries().forEach { (value, id) ->
                        w.write(id.toString())
                        w.write('\t'.code)
                        w.write(value.replace("\n", "\\n").replace("\t", "\\t"))
                        w.write('\n'.code)
                    }
                }
            }
        }
        // Member dict if applicable.
        if (cfg.member.encoding == Member.Encoding.DICT) {
            val md = seg.memberEncoder().dict()
            if (md != null) {
                Files.newBufferedWriter(dir.resolve("_member.dict")).use { w ->
                    md.entries().forEach { (value, id) ->
                        w.write(id.toString())
                        w.write('\t'.code)
                        w.write(value)
                        w.write('\n'.code)
                    }
                }
            }
        }
    }

    private fun writeMetrics(
        seg: Segment,
        dir: Path,
    ) {
        seg.ullSketches.forEach { (name, sliceMap) ->
            DataOutputStream(FileOutputStream(dir.resolve("$name.ull").toFile()).buffered()).use { out ->
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
        seg.counters.forEach { (name, v) ->
            DataOutputStream(FileOutputStream(dir.resolve("$name.count").toFile())).use { out -> out.writeLong(v.get()) }
        }
        seg.sums.forEach { (name, v) ->
            DataOutputStream(FileOutputStream(dir.resolve("$name.sum").toFile())).use { out -> out.writeLong(v.get()) }
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
                    val entries =
                        Files.readAllLines(f).mapNotNull { line ->
                            val parts = line.split('\t', limit = 2)
                            if (parts.size != 2) null else parts[1].replace("\\n", "\n") to parts[0].toInt()
                        }
                    // Rebuild dict by replacing
                    entries.forEach { (value, _) -> enc.dict().getOrIntern(value, null) }
                }
            }
        }

        // Dims
        val dimsDir = dir.resolve("dims")
        cfg.dimensions.forEach { d ->
            val f = dimsDir.resolve("${d.name}.rbi")
            if (Files.exists(f)) {
                DataInputStream(FileInputStream(f.toFile()).buffered()).use { inp ->
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
                        DataInputStream(FileInputStream(f.toFile()).buffered()).use { inp ->
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

        seg.freeze()
        return seg
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

package io.conduktor.kri.persistence

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.conduktor.kri.index.Segment
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant

/**
 * Flat catalog of every frozen segment for one indexer.
 * Single source of truth for "which .rbi files exist". Written atomically
 * on each rollover so query path never scans the segments directory.
 */
class SegmentManifest(
    private val root: Path,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val file: Path = root.resolve("manifest.json")
    private val tmp: Path = root.resolve("manifest.json.tmp")

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Entry(
        val id: Long,
        val partition: Int,
        val startMs: Long,
        val endMs: Long,
        val recordCount: Long,
        val dir: String,
        val offsets: Map<Int, OffsetRange> = emptyMap(),
        val dimValueCounts: Map<String, Int> = emptyMap(),
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class OffsetRange(
        val first: Long,
        val last: Long,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Doc(
        val version: Int = 1,
        val entries: List<Entry> = emptyList(),
    )

    @Synchronized
    fun load(): List<Entry> {
        if (!Files.exists(file)) return emptyList()
        return try {
            mapper.readValue<Doc>(Files.readAllBytes(file)).entries
        } catch (e: Exception) {
            log.warn("Manifest unreadable at {}: {}. Ignoring.", file, e.message)
            emptyList()
        }
    }

    @Synchronized
    fun save(entries: List<Entry>) {
        Files.createDirectories(root)
        val doc = Doc(version = 1, entries = entries.sortedWith(compareBy({ it.startMs }, { it.partition }, { it.id })))
        val json = mapper.writeValueAsBytes(doc)
        Files.write(tmp, json)
        java.nio.channels.FileChannel
            .open(tmp, java.nio.file.StandardOpenOption.WRITE)
            .use { it.force(true) }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    }

    @Synchronized
    fun upsert(entry: Entry) {
        val current = load().toMutableList()
        val idx = current.indexOfFirst { it.id == entry.id }
        if (idx >= 0) current[idx] = entry else current.add(entry)
        save(current)
    }

    fun overlapping(
        from: Instant,
        to: Instant,
        partitions: Set<Int>? = null,
    ): List<Entry> {
        val fromMs = from.toEpochMilli()
        val toMs = to.toEpochMilli()
        return load().filter { e ->
            e.startMs < toMs && e.endMs > fromMs && (partitions == null || e.partition in partitions)
        }
    }

    companion object {
        fun segmentToEntry(
            seg: Segment,
            dirName: String,
        ): Entry =
            Entry(
                id = seg.id,
                partition = seg.partition,
                startMs = seg.tStart.toEpochMilli(),
                endMs = seg.tEnd.toEpochMilli(),
                recordCount = seg.recordCount.get(),
                dir = dirName,
                offsets = seg.offsets.mapValues { OffsetRange(it.value.first, it.value.last) },
                dimValueCounts = seg.dims.mapValues { it.value.size },
            )

        private val mapper: ObjectMapper =
            ObjectMapper()
                .registerKotlinModule()
                .configure(SerializationFeature.INDENT_OUTPUT, true)
                .configure(MapperFeature.USE_STD_BEAN_NAMING, true)
    }
}

package io.conduktor.kri.persistence

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.index.SegmentStore
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterAst
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.QueryRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

class SegmentManifestTest {
    private fun yamlFor(path: Path): String =
        """
        apiVersion: indexer.conduktor.io/v1
        kind: IndexerConfig
        metadata: { name: manifest-test }
        source: { topic: t, bootstrap: b:9092, consumerGroup: g }
        segmentation: { strategy: time, bucket: 1h }
        fields:
          userId:  { path: userId,  type: uint32 }
          country: { path: country, type: string }
        member: { field: userId, encoding: raw_uint32 }
        dimensions:
          - { name: country, field: country, encoding: dict }
        metrics:
          - { name: requestCount, type: count }
        storage: { path: $path }
        """.trimIndent()

    @Test
    fun `manifest written on write contains all persisted segments`(
        @TempDir tmp: Path,
    ) {
        val cfg = ConfigLoader.parseOnly(yamlFor(tmp))
        val store = SegmentStore(cfg)
        val io = SegmentIO(cfg)
        val t0 = Instant.parse("2026-04-24T10:00:00Z")

        // 2 partitions × 2 buckets = 4 segments
        for (p in 0..1) {
            for (b in 0..1) {
                val ts = t0.plusSeconds((b * 3600).toLong())
                val seg = store.segmentFor(p, ts)
                seg.add(p, (p * 10 + b).toLong(), mapOf("userId" to (p * 100L + b), "country" to "FR"))
            }
        }
        store.forceRollAll { seg -> io.write(seg) }

        val entries = io.manifest.load()
        assertThat(entries).hasSize(4)
        assertThat(entries.map { it.partition }.toSet()).containsExactlyInAnyOrder(0, 1)
        assertThat(entries.all { it.recordCount == 1L }).isTrue
        assertThat(entries.all { Files.exists(tmp.resolve("segments").resolve(it.dir)) }).isTrue

        val manifestFile = tmp.resolve("manifest.json")
        assertThat(Files.exists(manifestFile)).isTrue
        val raw = Files.readString(manifestFile)
        assertThat(raw).contains("\"partition\"")
        assertThat(raw).contains("\"startMs\"")
    }

    @Test
    fun `new store loads segments via manifest and serves queries identically`(
        @TempDir tmp: Path,
    ) {
        val cfg = ConfigLoader.parseOnly(yamlFor(tmp))
        val ioWrite = SegmentIO(cfg)
        val storeWrite = SegmentStore(cfg)
        val t0 = Instant.parse("2026-04-24T10:00:00Z")

        // Distinct users across partitions with one overlap (user 42).
        storeWrite.segmentFor(0, t0).add(0, 0, mapOf("userId" to 1L, "country" to "FR"))
        storeWrite.segmentFor(0, t0).add(0, 1, mapOf("userId" to 42L, "country" to "FR"))
        storeWrite.segmentFor(1, t0).add(1, 0, mapOf("userId" to 42L, "country" to "FR"))
        storeWrite.segmentFor(1, t0).add(1, 1, mapOf("userId" to 7L, "country" to "DE"))
        storeWrite.forceRollAll { seg -> ioWrite.write(seg) }

        // Simulate fresh process restart.
        val storeRead = SegmentStore(cfg)
        val ioRead = SegmentIO(cfg)
        ioRead.loadAll(storeRead, AtomicLong(0))

        val loaded = storeRead.allSegments()
        assertThat(loaded).hasSize(2)
        assertThat(loaded.map { it.partition }).containsExactlyInAnyOrder(0, 1)

        val ev = Evaluator(cfg)
        try {
            val fr =
                ev.evaluate(
                    storeRead.segmentsOverlapping(t0, t0.plusSeconds(3600)),
                    QueryRequest(t0, t0.plusSeconds(3600), FilterParser.parse("country:FR"), "cardinality"),
                )
            // Users in FR: {1, 42} ∪ {42} = 2 distinct
            assertThat(fr.result).isEqualTo(2L)

            val all =
                ev.evaluate(
                    storeRead.segmentsOverlapping(t0, t0.plusSeconds(3600)),
                    QueryRequest(t0, t0.plusSeconds(3600), FilterAst.True, "cardinality"),
                )
            // All users deduped: {1,42,7} = 3
            assertThat(all.result).isEqualTo(3L)
        } finally {
            ev.shutdown()
        }
    }

    @Test
    fun `fallback directory scan rebuilds manifest when missing`(
        @TempDir tmp: Path,
    ) {
        val cfg = ConfigLoader.parseOnly(yamlFor(tmp))
        val io = SegmentIO(cfg)
        val store = SegmentStore(cfg)
        val t0 = Instant.parse("2026-04-24T10:00:00Z")
        store.segmentFor(0, t0).add(0, 0, mapOf("userId" to 1L, "country" to "FR"))
        store.forceRollAll { seg -> io.write(seg) }

        // Nuke manifest, simulate legacy directory layout.
        Files.deleteIfExists(tmp.resolve("manifest.json"))

        val store2 = SegmentStore(cfg)
        val io2 = SegmentIO(cfg)
        io2.loadAll(store2, AtomicLong(0))

        assertThat(store2.allSegments()).hasSize(1)
        // Manifest should have been rebuilt from scan.
        assertThat(Files.exists(tmp.resolve("manifest.json"))).isTrue
        assertThat(io2.manifest.load()).hasSize(1)
    }
}

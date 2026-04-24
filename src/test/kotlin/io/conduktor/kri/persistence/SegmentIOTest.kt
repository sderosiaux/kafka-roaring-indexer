package io.conduktor.kri.persistence

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.index.Segment
import io.conduktor.kri.index.SegmentStore
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.QueryRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

class SegmentIOTest {
    @Test
    fun `round-trip persists dims dicts metrics`(
        @TempDir tmp: Path,
    ) {
        val yaml =
            """
            apiVersion: indexer.conduktor.io/v1
            kind: IndexerConfig
            metadata: { name: persist-test }
            source: { topic: events, bootstrap: b:9092, consumerGroup: g }
            segmentation: { strategy: time, bucket: 1h }
            fields:
              userId:  { path: userId,  type: uint32 }
              country: { path: country, type: string }
            member: { field: userId, encoding: raw_uint32 }
            dimensions:
              - { name: country, field: country, encoding: dict }
            metrics:
              - { name: distinctUsers, type: ull, field: userId, precision: 12 }
              - { name: requestCount,  type: count }
            storage: { path: $tmp }
            """.trimIndent()
        val cfg = ConfigLoader.parseOnly(yaml)
        val seg = Segment.create(0, 0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), cfg)
        seg.add(0, 0, mapOf("userId" to 1L, "country" to "FR"))
        seg.add(0, 1, mapOf("userId" to 2L, "country" to "FR"))
        seg.add(0, 2, mapOf("userId" to 3L, "country" to "DE"))
        seg.freeze()

        val io = SegmentIO(cfg)
        io.write(seg)

        val store = SegmentStore(cfg)
        io.loadAll(store, AtomicLong(0))
        val loaded = store.allSegments().single()
        assertThat(loaded.dims["country"]).isNotNull
        assertThat(loaded.counters["requestCount"]?.get()).isEqualTo(3L)

        val ev = Evaluator(cfg)
        val frResp =
            ev.evaluate(
                listOf(loaded),
                QueryRequest(Instant.EPOCH, Instant.EPOCH.plusSeconds(7200), FilterParser.parse("country:FR"), "cardinality"),
            )
        assertThat(frResp.result).isEqualTo(2L)

        val deResp =
            ev.evaluate(
                listOf(loaded),
                QueryRequest(Instant.EPOCH, Instant.EPOCH.plusSeconds(7200), FilterParser.parse("country:DE"), "cardinality"),
            )
        assertThat(deResp.result).isEqualTo(1L)
    }
}

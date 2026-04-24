package io.conduktor.kri.index

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterAst
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.QueryRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class MultiPartitionTest {
    private val cfg =
        ConfigLoader.parseOnly(
            """
            apiVersion: indexer.conduktor.io/v1
            kind: IndexerConfig
            metadata: { name: multipart-test }
            source: { topic: t, bootstrap: b:9092, consumerGroup: g }
            segmentation: { strategy: time, bucket: 1h }
            fields:
              userId:  { path: userId,  type: uint32 }
              country: { path: country, type: string }
              device:  { path: device,  type: string }
            member: { field: userId, encoding: raw_uint32 }
            dimensions:
              - { name: country, field: country, encoding: dict }
              - { name: device,  field: device,  encoding: dict }
            metrics:
              - { name: requestCount, type: count }
            storage: { path: /tmp/kri-multipart }
            """.trimIndent(),
        )

    @Test
    fun `one segment per partition per time bucket`() {
        val store = SegmentStore(cfg)
        val t0 = Instant.parse("2026-04-24T10:00:00Z")

        // Two partitions, same bucket → expect 2 segments.
        val s0 = store.segmentFor(0, t0.plusSeconds(60))
        val s1 = store.segmentFor(1, t0.plusSeconds(120))
        assertThat(s0.id).isNotEqualTo(s1.id)
        assertThat(s0.partition).isEqualTo(0)
        assertThat(s1.partition).isEqualTo(1)
        assertThat(s0.tStart).isEqualTo(s1.tStart)

        // Same partition, same bucket → reuse.
        val s0Again = store.segmentFor(0, t0.plusSeconds(300))
        assertThat(s0Again).isSameAs(s0)

        // Different bucket, same partition → new segment.
        val s0Next = store.segmentFor(0, t0.plusSeconds(3700))
        assertThat(s0Next.id).isNotEqualTo(s0.id)
        assertThat(s0Next.partition).isEqualTo(0)

        assertThat(store.size()).isEqualTo(3)
    }

    @Test
    fun `unified query fans out across partitions and unions bitmaps`() {
        val store = SegmentStore(cfg)
        val t0 = Instant.parse("2026-04-24T10:00:00Z")

        // partition 0: users 1,2,3 in FR/mobile
        store.segmentFor(0, t0).add(0, 0, mapOf("userId" to 1L, "country" to "FR", "device" to "mobile"))
        store.segmentFor(0, t0).add(0, 1, mapOf("userId" to 2L, "country" to "FR", "device" to "mobile"))
        store.segmentFor(0, t0).add(0, 2, mapOf("userId" to 3L, "country" to "DE", "device" to "mobile"))

        // partition 1: users 3,4,5; note user 3 appears in BOTH partitions (shared userId)
        store.segmentFor(1, t0).add(1, 0, mapOf("userId" to 3L, "country" to "FR", "device" to "desktop"))
        store.segmentFor(1, t0).add(1, 1, mapOf("userId" to 4L, "country" to "FR", "device" to "mobile"))
        store.segmentFor(1, t0).add(1, 2, mapOf("userId" to 5L, "country" to "DE", "device" to "mobile"))

        store.forceRollAll()
        val segs = store.segmentsOverlapping(t0, t0.plusSeconds(3600))
        assertThat(segs).hasSize(2)

        val ev = Evaluator(cfg)
        try {
            // Distinct users in FR, across partitions: {1,2} ∪ {3,4} = 4
            val fr =
                ev.evaluate(
                    segs,
                    QueryRequest(t0, t0.plusSeconds(3600), FilterParser.parse("country:FR"), "cardinality"),
                )
            assertThat(fr.result).isEqualTo(4L)

            // Distinct users globally: {1,2,3} ∪ {3,4,5} = 5 (user 3 deduped!)
            val all =
                ev.evaluate(
                    segs,
                    QueryRequest(t0, t0.plusSeconds(3600), FilterAst.True, "cardinality"),
                )
            assertThat(all.result).isEqualTo(5L)

            // Sanity: matchedRecords sums per-segment cardinalities (no dedup) → 3 + 3 = 6
            assertThat(all.matchedRecords).isEqualTo(6L)

            // Distinct users FR AND mobile: p0 has {1,2}, p1 has {4} → 3 total
            val frMobile =
                ev.evaluate(
                    segs,
                    QueryRequest(t0, t0.plusSeconds(3600), FilterParser.parse("country:FR AND device:mobile"), "cardinality"),
                )
            assertThat(frMobile.result).isEqualTo(3L)

            // requestCount global: sums scalar counters across both segments
            val total =
                ev.evaluate(
                    segs,
                    QueryRequest(t0, t0.plusSeconds(3600), FilterAst.True, "requestCount"),
                )
            assertThat(total.result).isEqualTo(6L)
        } finally {
            ev.shutdown()
        }
    }

    @Test
    fun `segmentsOverlapping filters by partition subset`() {
        val store = SegmentStore(cfg)
        val t0 = Instant.parse("2026-04-24T10:00:00Z")
        store.segmentFor(0, t0)
        store.segmentFor(1, t0)
        store.segmentFor(2, t0)

        val only01 = store.segmentsOverlapping(t0, t0.plusSeconds(3600), partitions = setOf(0, 1))
        assertThat(only01).hasSize(2)
        assertThat(only01.map { it.partition }).containsExactlyInAnyOrder(0, 1)

        val all = store.segmentsOverlapping(t0, t0.plusSeconds(3600))
        assertThat(all).hasSize(3)
    }
}

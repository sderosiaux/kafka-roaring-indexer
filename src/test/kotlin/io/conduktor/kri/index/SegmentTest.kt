package io.conduktor.kri.index

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterAst
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.QueryRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class SegmentTest {
    private val cfgYaml =
        """
        apiVersion: indexer.conduktor.io/v1
        kind: IndexerConfig
        metadata: { name: test-analytics }
        source: { topic: events, bootstrap: b:9092, consumerGroup: g }
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
          - { name: distinctUsers, type: ull, field: userId, precision: 12 }
          - { name: requestCount,  type: count }
        storage: { path: /tmp/kri-test }
        """.trimIndent()

    private fun cfg(): IndexerConfig = ConfigLoader.parseOnly(cfgYaml)

    @Test
    fun `add and filter single dim`() {
        val c = cfg()
        val t0 = Instant.parse("2026-04-24T10:00:00Z")
        val seg = Segment.create(0, t0, t0.plusSeconds(3600), c)
        seg.add(0, 0, mapOf("userId" to 1L, "country" to "FR", "device" to "mobile"))
        seg.add(0, 1, mapOf("userId" to 2L, "country" to "FR", "device" to "desktop"))
        seg.add(0, 2, mapOf("userId" to 3L, "country" to "DE", "device" to "mobile"))
        seg.add(0, 3, mapOf("userId" to 1L, "country" to "FR", "device" to "mobile")) // duplicate user

        val ev = Evaluator(c)
        val frBitmap = ev.evalFilter(seg, FilterParser.parse("country:FR"))
        assertThat(frBitmap.longCardinality).isEqualTo(2) // user 1 + user 2, user 1 deduped
    }

    @Test
    fun `AND filter across dims`() {
        val c = cfg()
        val seg = Segment.create(0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), c)
        seg.add(0, 0, mapOf("userId" to 1L, "country" to "FR", "device" to "mobile"))
        seg.add(0, 1, mapOf("userId" to 2L, "country" to "FR", "device" to "desktop"))
        seg.add(0, 2, mapOf("userId" to 3L, "country" to "DE", "device" to "mobile"))

        val ev = Evaluator(c)
        val r = ev.evalFilter(seg, FilterParser.parse("country:FR AND device:mobile"))
        assertThat(r.longCardinality).isEqualTo(1) // only user 1
    }

    @Test
    fun `OR expansion on dim values`() {
        val c = cfg()
        val seg = Segment.create(0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), c)
        seg.add(0, 0, mapOf("userId" to 1L, "country" to "FR", "device" to "mobile"))
        seg.add(0, 1, mapOf("userId" to 2L, "country" to "DE", "device" to "desktop"))
        seg.add(0, 2, mapOf("userId" to 3L, "country" to "IT", "device" to "mobile"))

        val ev = Evaluator(c)
        val r = ev.evalFilter(seg, FilterParser.parse("country:FR,DE"))
        assertThat(r.longCardinality).isEqualTo(2)
    }

    @Test
    fun `NOT flip`() {
        val c = cfg()
        val seg = Segment.create(0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), c)
        seg.add(0, 0, mapOf("userId" to 1L, "country" to "FR", "device" to "mobile"))
        seg.add(0, 1, mapOf("userId" to 2L, "country" to "DE", "device" to "desktop"))
        seg.add(0, 2, mapOf("userId" to 3L, "country" to "IT", "device" to "mobile"))

        val ev = Evaluator(c)
        val r = ev.evalFilter(seg, FilterParser.parse("NOT country:FR"))
        assertThat(r.longCardinality).isEqualTo(2) // user 2, user 3
    }

    @Test
    fun `cardinality query returns distinct user count`() {
        val c = cfg()
        val seg = Segment.create(0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), c)
        repeat(10) { i ->
            seg.add(0, i.toLong(), mapOf("userId" to (i % 5).toLong(), "country" to "FR", "device" to "mobile"))
        }
        val ev = Evaluator(c)
        val resp = ev.evaluate(listOf(seg), QueryRequest(Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), FilterAst.True, "cardinality"))
        assertThat(resp.result).isEqualTo(5L)
    }

    @Test
    fun `count metric accumulates`() {
        val c = cfg()
        val seg = Segment.create(0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), c)
        repeat(7) { i -> seg.add(0, i.toLong(), mapOf("userId" to i.toLong(), "country" to "FR", "device" to "mobile")) }
        val ev = Evaluator(c)
        val resp = ev.evaluate(listOf(seg), QueryRequest(Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), FilterAst.True, "requestCount"))
        assertThat(resp.result).isEqualTo(7L)
    }
}

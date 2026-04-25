package io.conduktor.kri.query

import com.fasterxml.jackson.databind.ObjectMapper
import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.index.SegmentStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Instant

/**
 * Integration tests for Option C multi-consumer fanout.
 * Spins up real Javalin servers in-process; no Kafka, no disk.
 */
class HttpFanoutTest {
    private val http = HttpClient.newHttpClient()
    private val mapper = ObjectMapper()
    private val t0 = Instant.parse("2026-04-24T10:00:00Z")

    private fun freePort(): Int = ServerSocket(0).use { it.localPort }

    private fun yaml(
        port: Int,
        peerPorts: List<Int> = emptyList(),
    ): String {
        val base =
            """
            apiVersion: indexer.conduktor.io/v1
            kind: IndexerConfig
            metadata: { name: fanout-test }
            source: { topic: events, bootstrap: b:9092, consumerGroup: g }
            segmentation: { strategy: time, bucket: 1h }
            fields:
              userId:  { path: userId,  type: uint32 }
              country: { path: country, type: string }
            member: { field: userId, encoding: raw_uint32 }
            dimensions:
              - { name: country, field: country, encoding: dict }
            metrics:
              - { name: requestCount, type: count }
            storage: { path: /tmp/kri-fanout }
            query:
              http:
                bind: "127.0.0.1:$port"
            """.trimIndent()
        if (peerPorts.isEmpty()) return base
        val peers = peerPorts.joinToString("\n") { "    - \"http://127.0.0.1:$it\"" }
        return "$base\n  peers:\n$peers"
    }

    /** Builds an HttpServer with in-memory segments populated from [events]. */
    private fun pod(
        port: Int,
        vararg events: Map<String, Any>,
        peerPorts: List<Int> = emptyList(),
    ): HttpServer {
        val cfg = ConfigLoader.parseOnly(yaml(port, peerPorts))
        val store = SegmentStore(cfg)
        events.forEachIndexed { i, evt ->
            store.segmentFor(0, t0).add(0, i.toLong(), evt)
        }
        store.forceRollAll()
        return HttpServer(cfg, store, Evaluator(cfg))
    }

    private fun get(
        port: Int,
        path: String,
    ): Map<*, *> {
        val resp =
            http.send(
                HttpRequest.newBuilder(URI("http://127.0.0.1:$port$path")).build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        assertThat(resp.statusCode()).isEqualTo(200)
        return mapper.readValue(resp.body(), Map::class.java)
    }

    @Test
    fun `count fanout sums events across all pods`() {
        val p1 = freePort()
        val p2 = freePort()
        val pc = freePort()
        val pod1 =
            pod(
                p1,
                mapOf("userId" to 1L, "country" to "FR"),
                mapOf("userId" to 2L, "country" to "FR"),
                mapOf("userId" to 3L, "country" to "DE"),
            )
        val pod2 =
            pod(
                p2,
                mapOf("userId" to 4L, "country" to "FR"),
                mapOf("userId" to 5L, "country" to "DE"),
            )
        val coord = pod(pc, peerPorts = listOf(p1, p2))
        try {
            pod1.start()
            pod2.start()
            coord.start()
            val result = get(pc, "/query?agg=count&from=2026-04-24T10:00:00Z&to=2026-04-24T11:00:00Z")
            assertThat((result["result"] as Number).toLong()).isEqualTo(5L)
            assertThat((result["segments"] as Number).toInt()).isEqualTo(2)
        } finally {
            coord.stop()
            pod1.stop()
            pod2.stop()
        }
    }

    @Test
    fun `cardinality fanout unions bitmaps and deduplicates members across pods`() {
        val p1 = freePort()
        val p2 = freePort()
        val pc = freePort()
        // userId=3 appears in BOTH pods — must be counted once, not twice.
        val pod1 =
            pod(
                p1,
                mapOf("userId" to 1L, "country" to "FR"),
                mapOf("userId" to 2L, "country" to "FR"),
                mapOf("userId" to 3L, "country" to "DE"),
            )
        val pod2 =
            pod(
                p2,
                mapOf("userId" to 3L, "country" to "FR"),
                mapOf("userId" to 4L, "country" to "FR"),
                mapOf("userId" to 5L, "country" to "DE"),
            )
        val coord = pod(pc, peerPorts = listOf(p1, p2))
        try {
            pod1.start()
            pod2.start()
            coord.start()
            val result = get(pc, "/query?agg=cardinality&from=2026-04-24T10:00:00Z&to=2026-04-24T11:00:00Z")
            // {1,2,3} ∪ {3,4,5} = 5, not 6
            assertThat((result["result"] as Number).toLong()).isEqualTo(5L)
        } finally {
            coord.stop()
            pod1.stop()
            pod2.stop()
        }
    }

    @Test
    fun `cardinality fanout respects filter across pods`() {
        val p1 = freePort()
        val p2 = freePort()
        val pc = freePort()
        val pod1 =
            pod(
                p1,
                mapOf("userId" to 1L, "country" to "FR"),
                mapOf("userId" to 2L, "country" to "DE"),
            )
        val pod2 =
            pod(
                p2,
                mapOf("userId" to 3L, "country" to "FR"),
                mapOf("userId" to 4L, "country" to "US"),
            )
        val coord = pod(pc, peerPorts = listOf(p1, p2))
        try {
            pod1.start()
            pod2.start()
            coord.start()
            val result =
                get(pc, "/query?agg=cardinality&filter=country:FR&from=2026-04-24T10:00:00Z&to=2026-04-24T11:00:00Z")
            // FR users: {1} from pod1, {3} from pod2 → 2
            assertThat((result["result"] as Number).toLong()).isEqualTo(2L)
        } finally {
            coord.stop()
            pod1.stop()
            pod2.stop()
        }
    }

    @Test
    fun `partial flag set when a peer is unreachable`() {
        val p1 = freePort()
        val dead = freePort()
        val pc = freePort()
        // dead port is never started — connect will fail immediately
        val pod1 =
            pod(
                p1,
                mapOf("userId" to 1L, "country" to "FR"),
                mapOf("userId" to 2L, "country" to "FR"),
            )
        val coord = pod(pc, peerPorts = listOf(p1, dead))
        try {
            pod1.start()
            coord.start()
            val result = get(pc, "/query?agg=count&from=2026-04-24T10:00:00Z&to=2026-04-24T11:00:00Z")
            assertThat(result["partial"]).isEqualTo(true)
            assertThat((result["result"] as Number).toLong()).isEqualTo(2L)
        } finally {
            coord.stop()
            pod1.stop()
        }
    }

    @Test
    fun `facets fanout sums per-dimension counts across pods`() {
        val p1 = freePort()
        val p2 = freePort()
        val pc = freePort()
        val pod1 =
            pod(
                p1,
                mapOf("userId" to 1L, "country" to "FR"),
                mapOf("userId" to 2L, "country" to "FR"),
                mapOf("userId" to 3L, "country" to "DE"),
            )
        val pod2 =
            pod(
                p2,
                mapOf("userId" to 4L, "country" to "FR"),
                mapOf("userId" to 5L, "country" to "US"),
            )
        val coord = pod(pc, peerPorts = listOf(p1, p2))
        try {
            pod1.start()
            pod2.start()
            coord.start()
            val result = get(pc, "/facets?from=2026-04-24T10:00:00Z&to=2026-04-24T11:00:00Z")

            @Suppress("UNCHECKED_CAST")
            val country = (result["facets"] as Map<String, List<Map<String, Any>>>)["country"]!!
            val counts = country.associate { it["value"] to (it["count"] as Number).toLong() }
            assertThat(counts["FR"]).isEqualTo(3L) // 2 from pod1 + 1 from pod2
            assertThat(counts["DE"]).isEqualTo(1L)
            assertThat(counts["US"]).isEqualTo(1L)
        } finally {
            coord.stop()
            pod1.stop()
            pod2.stop()
        }
    }
}

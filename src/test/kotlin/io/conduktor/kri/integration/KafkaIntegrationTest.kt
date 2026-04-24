package io.conduktor.kri.integration

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.index.SegmentStore
import io.conduktor.kri.ingest.IndexerConsumer
import io.conduktor.kri.persistence.SegmentIO
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.QueryRequest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.nio.file.Path
import java.time.Instant
import java.util.Properties

class KafkaIntegrationTest {
    companion object {
        @JvmStatic
        private var kafka: KafkaContainer? = null

        @JvmStatic
        @BeforeAll
        fun startKafka() {
            val container = KafkaContainer(DockerImageName.parse("apache/kafka:3.9.0"))
            try {
                container.start()
                kafka = container
            } catch (e: Throwable) {
                Assumptions.abort<Unit>("Docker unavailable — skipping integration test: ${e.message}")
            }
        }

        @JvmStatic
        @AfterAll
        fun stopKafka() {
            kafka?.stop()
        }
    }

    private fun broker() = kafka?.bootstrapServers ?: error("kafka not started")

    @Test
    fun `consume JSON topic and answer cardinality query`(
        @TempDir tmp: Path,
    ) {
        val topic = "kri-it-events"
        createTopic(topic)
        produce(
            topic,
            listOf(
                """{"userId":1,"country":"FR","device":"mobile"}""",
                """{"userId":2,"country":"FR","device":"desktop"}""",
                """{"userId":3,"country":"DE","device":"mobile"}""",
                """{"userId":1,"country":"FR","device":"mobile"}""",
            ),
        )

        val yaml =
            """
            apiVersion: indexer.conduktor.io/v1
            kind: IndexerConfig
            metadata: { name: it-config }
            source:
              topic: $topic
              bootstrap: ${broker()}
              consumerGroup: kri-it
              startOffset: earliest
              schemaRegistry: { url: "http://unused:8081", format: json }
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
            storage: { path: $tmp }
            """.trimIndent()

        val cfg = ConfigLoader.parseOnly(yaml)
        val store = SegmentStore(cfg)
        val io = SegmentIO(cfg)
        val consumer = IndexerConsumer(cfg, store, io)
        consumer.start()

        // Poll until we see at least 4 records or timeout.
        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline) {
            val total = store.allSegments().sumOf { it.recordCount.get() }
            if (total >= 4) break
            Thread.sleep(200)
        }
        consumer.stop()

        val ev = Evaluator(cfg)
        val segs = store.allSegments()
        assertThat(segs.sumOf { it.recordCount.get() }).isGreaterThanOrEqualTo(4L)

        val fr =
            ev.evaluate(
                segs,
                QueryRequest(
                    Instant.EPOCH,
                    Instant.now().plusSeconds(60),
                    FilterParser.parse("country:FR AND device:mobile"),
                    "cardinality",
                ),
            )
        assertThat(fr.result).isEqualTo(1L) // only userId=1 matches FR+mobile
    }

    private fun createTopic(name: String) {
        val props = Properties().apply { put("bootstrap.servers", broker()) }
        AdminClient.create(props).use { a ->
            a.createTopics(listOf(NewTopic(name, 1, 1.toShort()))).all().get()
        }
    }

    private fun produce(
        topic: String,
        values: List<String>,
    ) {
        val props =
            Properties().apply {
                put("bootstrap.servers", broker())
                put("key.serializer", StringSerializer::class.java.name)
                put("value.serializer", StringSerializer::class.java.name)
                put("acks", "all")
            }
        KafkaProducer<String, String>(props).use { p ->
            values.forEach { v -> p.send(ProducerRecord(topic, null, v)).get() }
            p.flush()
        }
    }
}

package io.conduktor.kri.config

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

class ConfigLoaderTest {
    @Test
    fun `loads example events-analytics yaml`() {
        val res = ConfigLoaderTest::class.java.classLoader.getResource("events-analytics.yaml")!!
        val cfg = ConfigLoader.loadAndValidate(Path.of(res.toURI()))
        assertThat(cfg.metadata.name).isEqualTo("events-analytics-poc")
        assertThat(cfg.dimensions).extracting("name").contains("country", "device", "status", "latencyBucket", "pathHashed")
        assertThat(cfg.metrics).extracting("name").contains("distinctUsers", "distinctPaths", "requestCount", "totalLatencyMs")
    }

    @Test
    fun `parseOnly accepts minimal valid doc`() {
        val y =
            """
            apiVersion: indexer.conduktor.io/v1
            kind: IndexerConfig
            metadata: { name: minimal }
            source: { topic: t, bootstrap: b:9092, consumerGroup: g }
            segmentation: { strategy: time, bucket: 1h }
            fields: { u: { path: "u", type: uint32 } }
            member: { field: u, encoding: raw_uint32 }
            dimensions: [{ name: d, field: u, encoding: raw_uint32 }]
            storage: { path: /tmp/kri }
            """.trimIndent()
        val cfg = ConfigLoader.parseOnly(y)
        assertThat(cfg.dimensions).hasSize(1)
    }

    @Test
    fun `rejects unknown member field`() {
        val y =
            """
            apiVersion: indexer.conduktor.io/v1
            kind: IndexerConfig
            metadata: { name: bad }
            source: { topic: t, bootstrap: b:9092, consumerGroup: g }
            segmentation: { strategy: time, bucket: 1h }
            fields: { u: { path: "u", type: uint32 } }
            member: { field: missing, encoding: raw_uint32 }
            dimensions: [{ name: d, field: u, encoding: raw_uint32 }]
            storage: { path: /tmp/kri }
            """.trimIndent()
        val tmp = Files.createTempFile("cfg", ".yaml")
        Files.writeString(tmp, y)
        assertThatThrownBy { ConfigLoader.loadAndValidate(tmp) }
            .isInstanceOf(ConfigValidationException::class.java)
            .hasMessageContaining("member.field 'missing'")
    }
}

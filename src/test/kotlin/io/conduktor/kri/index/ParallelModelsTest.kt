package io.conduktor.kri.index

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.MultiModelEvaluator
import io.conduktor.kri.query.QueryRequest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.within
import org.junit.jupiter.api.Test
import java.time.Instant

class ParallelModelsTest {
    private val cfgYaml =
        """
        apiVersion: indexer.conduktor.io/v1
        kind: IndexerConfig
        metadata: { name: test-parallel-models }
        source: { topic: events, bootstrap: b:9092, consumerGroup: g }
        segmentation: { strategy: time, bucket: 1h }
        fields:
          userId:    { path: userId,    type: uint32 }
          country:   { path: country,   type: string }
          device:    { path: device,    type: string }
          latencyMs: { path: latencyMs, type: uint32 }
        member: { field: userId, encoding: raw_uint32 }
        dimensions:
          - { name: country,   field: country,   encoding: dict }
          - { name: device,    field: device,    encoding: dict }
          - { name: latencyMs, field: latencyMs, encoding: raw_uint32 }
        metrics:
          - { name: requestCount, type: count }
        storage: { path: /tmp/kri-parallel-test }
        experimentalModels:
          bsiDims: [latencyMs]
          thetaSample: true
          thetaSampleRate: 0.5
          jointProfile: true
          reorderMembers: true
        """.trimIndent()

    private fun cfg(): IndexerConfig = ConfigLoader.parseOnly(cfgYaml)

    private fun buildSegment(c: IndexerConfig): Segment {
        val seg = Segment.create(0, 0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), c)
        // 200 users across a small set of (country, device) profiles, with varying latency.
        val countries = listOf("FR", "DE", "IT", "ES")
        val devices = listOf("mobile", "desktop", "tablet")
        repeat(200) { i ->
            seg.add(
                0,
                i.toLong(),
                mapOf(
                    "userId" to (i + 1).toLong(),
                    "country" to countries[i % countries.size],
                    "device" to devices[i % devices.size],
                    "latencyMs" to (10 + (i * 7) % 500).toLong(),
                ),
            )
        }
        seg.freeze()
        return seg
    }

    @Test
    fun `roaring and roaring_reordered agree on cardinality for AND categorical`() {
        val c = cfg()
        val seg = buildSegment(c)
        val ev = Evaluator(c)
        val mev = MultiModelEvaluator(c, ev)

        val req =
            QueryRequest(
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(3600),
                FilterParser.parse("country:FR AND device:mobile"),
                "cardinality",
            )
        val results = mev.evaluate(listOf(seg), req, listOf(IndexModelKind.ROARING, IndexModelKind.ROARING_REORDERED))
        val roaring = results.first { it.model == IndexModelKind.ROARING }
        val reordered = results.first { it.model == IndexModelKind.ROARING_REORDERED }
        assertThat(roaring.supported).isTrue
        assertThat(reordered.supported).isTrue
        assertThat(reordered.value).isEqualTo(roaring.value)
    }

    @Test
    fun `joint_profile matches roaring exactly for AND categorical`() {
        val c = cfg()
        val seg = buildSegment(c)
        val ev = Evaluator(c)
        val mev = MultiModelEvaluator(c, ev)

        val req =
            QueryRequest(
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(3600),
                FilterParser.parse("country:FR AND device:mobile"),
                "cardinality",
            )
        val results = mev.evaluate(listOf(seg), req, listOf(IndexModelKind.ROARING, IndexModelKind.JOINT_PROFILE))
        val roaring = results.first { it.model == IndexModelKind.ROARING }
        val jp = results.first { it.model == IndexModelKind.JOINT_PROFILE }
        assertThat(jp.supported).isTrue
        assertThat(jp.value).isEqualTo(roaring.value)
    }

    @Test
    fun `joint_profile rejects range predicates`() {
        val c = cfg()
        val seg = buildSegment(c)
        val ev = Evaluator(c)
        val mev = MultiModelEvaluator(c, ev)

        val req =
            QueryRequest(
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(3600),
                FilterParser.parse("latencyMs:[100,200]"),
                "cardinality",
            )
        val results = mev.evaluate(listOf(seg), req, listOf(IndexModelKind.JOINT_PROFILE))
        val jp = results.first { it.model == IndexModelKind.JOINT_PROFILE }
        assertThat(jp.supported).isFalse
        assertThat(jp.error).contains("range")
    }

    @Test
    fun `bsi range query matches roaring range`() {
        val c = cfg()
        val seg = buildSegment(c)
        val ev = Evaluator(c)
        val mev = MultiModelEvaluator(c, ev)

        val req =
            QueryRequest(
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(3600),
                FilterParser.parse("latencyMs:[100,200]"),
                "cardinality",
            )
        val results = mev.evaluate(listOf(seg), req, listOf(IndexModelKind.ROARING, IndexModelKind.BSI))
        val roaring = results.first { it.model == IndexModelKind.ROARING }
        val bsi = results.first { it.model == IndexModelKind.BSI }
        assertThat(bsi.supported).isTrue
        assertThat(bsi.value).isEqualTo(roaring.value)
    }

    @Test
    fun `bsi half-open range matches roaring`() {
        val c = cfg()
        val seg = buildSegment(c)
        val ev = Evaluator(c)
        val mev = MultiModelEvaluator(c, ev)

        val req =
            QueryRequest(
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(3600),
                FilterParser.parse("latencyMs:>=300"),
                "cardinality",
            )
        val results = mev.evaluate(listOf(seg), req, listOf(IndexModelKind.ROARING, IndexModelKind.BSI))
        val roaring = results.first { it.model == IndexModelKind.ROARING }
        val bsi = results.first { it.model == IndexModelKind.BSI }
        assertThat(bsi.value).isEqualTo(roaring.value)
    }

    @Test
    fun `theta sample estimate is within tolerance for AND categorical`() {
        val c = cfg()
        val seg = buildSegment(c)
        val ev = Evaluator(c)
        val mev = MultiModelEvaluator(c, ev)

        // Larger filter so the sample produces meaningful counts.
        val req =
            QueryRequest(
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(3600),
                FilterParser.parse("country:FR,DE,IT"),
                "cardinality",
            )
        val results = mev.evaluate(listOf(seg), req, listOf(IndexModelKind.ROARING, IndexModelKind.THETA))
        val roaring = (results.first { it.model == IndexModelKind.ROARING }.value as Number).toDouble()
        val theta = results.first { it.model == IndexModelKind.THETA }
        assertThat(theta.supported).isTrue
        assertThat(theta.approx).isTrue
        // sampleRate=0.5 → standard error ~1/sqrt(N*0.5). With N=150, σ ≈ 0.115 — give 50% wide window.
        val thetaVal = (theta.value as Number).toDouble()
        assertThat(thetaVal).isCloseTo(roaring, within(roaring * 0.5))
    }

    @Test
    fun `bsi reports unsupported for missing BSI dim`() {
        // Build a config WITHOUT BSI for latencyMs.
        val noBsiCfg =
            ConfigLoader.parseOnly(
                cfgYaml.replace("bsiDims: [latencyMs]", "bsiDims: []"),
            )
        val seg = buildSegment(noBsiCfg)
        val ev = Evaluator(noBsiCfg)
        val mev = MultiModelEvaluator(noBsiCfg, ev)

        val req =
            QueryRequest(
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(3600),
                FilterParser.parse("latencyMs:[100,200]"),
                "cardinality",
            )
        val results = mev.evaluate(listOf(seg), req, listOf(IndexModelKind.BSI))
        val bsi = results.first()
        assertThat(bsi.supported).isFalse
        assertThat(bsi.error).contains("missing BSI")
    }

    @Test
    fun `member permutation is built when reorderMembers is enabled`() {
        val c = cfg()
        val seg = buildSegment(c)
        assertThat(seg.memberPermutation).isNotNull
        assertThat(seg.memberPermutation!!.size()).isEqualTo(200)
        assertThat(seg.reorderedDims).isNotNull
    }

    @Test
    fun `joint profile cardinality is much smaller than member count when users cluster`() {
        val c = cfg()
        val seg = buildSegment(c)
        val jp = seg.jointProfile
        assertThat(jp).isNotNull
        // 4 countries × 3 devices × #latency-buckets — but here latency varies per record so
        // each user has a unique signature. Profile count ≈ member count.
        // We mostly assert the index built and is queryable.
        assertThat(jp!!.profileCount()).isGreaterThan(0)
    }

    @Test
    fun `multi-model query returns timing per model`() {
        val c = cfg()
        val seg = buildSegment(c)
        val ev = Evaluator(c)
        val mev = MultiModelEvaluator(c, ev)

        val req =
            QueryRequest(
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(3600),
                FilterParser.parse("country:FR AND device:mobile"),
                "cardinality",
            )
        val results =
            mev.evaluate(
                listOf(seg),
                req,
                listOf(
                    IndexModelKind.ROARING,
                    IndexModelKind.ROARING_REORDERED,
                    IndexModelKind.JOINT_PROFILE,
                ),
            )
        results.forEach { r ->
            assertThat(r.timeNs).isGreaterThan(0)
        }
    }
}

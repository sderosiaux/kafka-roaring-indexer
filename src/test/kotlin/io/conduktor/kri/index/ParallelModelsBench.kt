package io.conduktor.kri.index

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.persistence.SegmentIO
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.MultiModelEvaluator
import io.conduktor.kri.query.QueryRequest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.DataOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

/**
 * Side-by-side comparison of parallel index models. Not an assertion test — prints a table to
 * stdout so you can read absolute and relative numbers per model, per query shape, per scale.
 *
 * Run with: gradle test --tests "ParallelModelsBench" --info
 *
 * Reports per-model:
 *   - p50 query latency (median of 100 runs after 10 warmup)
 *   - in-memory byte size of the model's structures
 *   - on-disk byte size after serialization
 *   - exact result (or estimate + relative error vs Roaring ground truth)
 */
class ParallelModelsBench {
    private val cfgYaml =
        """
        apiVersion: indexer.conduktor.io/v1
        kind: IndexerConfig
        metadata: { name: bench }
        source: { topic: events, bootstrap: b:9092, consumerGroup: g }
        segmentation: { strategy: time, bucket: 1h }
        fields:
          userId:    { path: userId,    type: uint32 }
          country:   { path: country,   type: string }
          device:    { path: device,    type: string }
          plan:      { path: plan,      type: string }
          status:    { path: status,    type: string }
          latencyMs: { path: latencyMs, type: uint32 }
        member: { field: userId, encoding: raw_uint32 }
        dimensions:
          - { name: country,   field: country,   encoding: dict }
          - { name: device,    field: device,    encoding: dict }
          - { name: plan,      field: plan,      encoding: dict }
          - { name: status,    field: status,    encoding: dict }
          - { name: latencyMs, field: latencyMs, encoding: raw_uint32 }
        metrics:
          - { name: requestCount, type: count }
        storage: { path: /tmp/kri-bench }
        experimentalModels:
          bsiDims: [latencyMs]
          thetaSample: true
          thetaSampleRate: 0.0625
          jointProfile: true
          reorderMembers: true
        """.trimIndent()

    private fun cfg(): IndexerConfig = ConfigLoader.parseOnly(cfgYaml)

    /**
     * Workload generator. Each user has FIXED (country, device, plan, latencyMs) attributes —
     * users cluster into cohorts so joint-profile shines, and BSI's single-value-per-member
     * semantics match Roaring exactly.
     *
     * NOTE on BSI: this BSI implementation is single-valued (last-write-wins per memberId). It is
     * the right structure for stable per-entity attributes (signup cohort, account age, plan tier
     * as numeric, etc.). For per-EVENT numeric metrics where the same user has many different
     * values across records (e.g. latency varying per request), Roaring per-(valueId, user) bitmaps
     * are the right structure and BSI is not a drop-in replacement.
     */
    private fun buildSegment(
        c: IndexerConfig,
        nUsers: Int,
        recordsPerUser: Int,
    ): Segment {
        val seg = Segment.create(0, 0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), c)
        val countries = listOf("FR", "DE", "IT", "ES", "UK", "NL", "BE", "PT")
        val devices = listOf("mobile", "desktop", "tablet")
        val plans = listOf("free", "pro", "enterprise")
        val statuses = listOf("200", "201", "204", "301", "404", "500", "503")

        val rng = java.util.Random(42L)

        // Each user is assigned fixed (country, device, plan, latencyMs).
        data class UserAttrs(
            val country: String,
            val device: String,
            val plan: String,
            val latencyMs: Long,
        )
        val userAttrs =
            (1..nUsers).associateWith {
                UserAttrs(
                    countries[rng.nextInt(countries.size)],
                    devices[rng.nextInt(devices.size)],
                    plans[rng.nextInt(plans.size)],
                    (5 + rng.nextInt(995)).toLong(),
                )
            }

        var off = 0L
        for (u in 1..nUsers) {
            val a = userAttrs.getValue(u)
            repeat(recordsPerUser) {
                seg.add(
                    0,
                    off++,
                    mapOf(
                        "userId" to u.toLong(),
                        "country" to a.country,
                        "device" to a.device,
                        "plan" to a.plan,
                        // status is the only per-event-varying dim — kept so realistic records
                        // exist but it's not part of any benchmark query.
                        "status" to statuses[rng.nextInt(statuses.size)],
                        "latencyMs" to a.latencyMs,
                    ),
                )
            }
        }
        seg.freeze()
        return seg
    }

    private fun timeP50(
        warmups: Int,
        runs: Int,
        block: () -> Unit,
    ): Long {
        repeat(warmups) { block() }
        val samples = LongArray(runs)
        for (i in 0 until runs) {
            val t0 = System.nanoTime()
            block()
            samples[i] = System.nanoTime() - t0
        }
        samples.sort()
        return samples[runs / 2]
    }

    private fun bitmapBytes(bm: org.roaringbitmap.RoaringBitmap): Long {
        val buf = java.io.ByteArrayOutputStream()
        bm.serialize(DataOutputStream(buf))
        return buf.size().toLong()
    }

    private fun roaringDimsBytes(dims: Map<String, Map<Int, org.roaringbitmap.RoaringBitmap>>): Long =
        dims.values.sumOf { vm -> vm.values.sumOf { bitmapBytes(it) } }

    private fun bsiBytes(bsi: BitSlicedIndex): Long {
        val buf = java.io.ByteArrayOutputStream()
        bsi.serialize(DataOutputStream(buf))
        return buf.size().toLong()
    }

    private fun thetaBytes(th: ThetaSampleIndex): Long {
        val buf = java.io.ByteArrayOutputStream()
        th.serialize(DataOutputStream(buf))
        return buf.size().toLong()
    }

    private fun jpBytes(jp: JointProfileIndex): Long {
        val buf = java.io.ByteArrayOutputStream()
        jp.serialize(DataOutputStream(buf))
        return buf.size().toLong()
    }

    private fun mb(bytes: Long): String = "%.2f MB".format(bytes / (1024.0 * 1024.0))

    private fun us(ns: Long): String = "%.2f µs".format(ns / 1000.0)

    @Test
    fun `compare models across query shapes and scales`(
        @TempDir tmp: Path,
    ) {
        // Override storage path for the persistence size measurement.
        val c = ConfigLoader.parseOnly(cfgYaml.replace("/tmp/kri-bench", tmp.toAbsolutePath().toString()))
        val ev = Evaluator(c)
        val mev = MultiModelEvaluator(c, ev)

        val scales =
            listOf(
                10_000 to 5, // 50k records, 10k users
                50_000 to 5, // 250k records, 50k users
            )

        val queries =
            listOf(
                "AND_CATEGORICAL_TIGHT" to FilterParser.parse("country:FR AND device:mobile AND plan:pro"),
                "AND_CATEGORICAL_LOOSE" to FilterParser.parse("country:FR,DE,IT"),
                "RANGE_NARROW" to FilterParser.parse("latencyMs:[100,200]"),
                "RANGE_WIDE" to FilterParser.parse("latencyMs:[50,800]"),
                "MIXED" to FilterParser.parse("country:FR AND latencyMs:[100,300]"),
            )

        val out = StringBuilder()
        out.appendLine()
        out.appendLine("=".repeat(110))
        out.appendLine("PARALLEL INDEX MODELS — SIDE-BY-SIDE BENCHMARK")
        out.appendLine("=".repeat(110))

        for ((nUsers, rpu) in scales) {
            val nRecords = nUsers * rpu
            val seg = buildSegment(c, nUsers, rpu)
            out.appendLine()
            out.appendLine("─".repeat(110))
            out.appendLine("SCALE: $nUsers users × $rpu records/user = $nRecords records")
            out.appendLine("─".repeat(110))

            // Sizes (in-memory, serialized).
            val roaringSz = roaringDimsBytes(seg.dims)
            val reorderSz = seg.reorderedDims?.let { roaringDimsBytes(it) } ?: 0L
            val bsiSz = seg.bsi.values.sumOf { bsiBytes(it) }
            val thetaSz = seg.theta?.let { thetaBytes(it) } ?: 0L
            val jpSz = seg.jointProfile?.let { jpBytes(it) } ?: 0L

            out.appendLine()
            out.appendLine("SERIALIZED SIZE:")
            out.appendLine("  Roaring (canonical) dims      : ${mb(roaringSz)}  ($roaringSz B)")
            out.appendLine(
                "  Roaring REORDERED dims        : ${mb(reorderSz)}  ($reorderSz B)  → Δ ${"%.1f".format(
                    (reorderSz - roaringSz) * 100.0 / roaringSz,
                )}%",
            )
            out.appendLine("  BSI (latencyMs)               : ${mb(bsiSz)}  ($bsiSz B)")
            out.appendLine("  Theta sample (sampleRate=6.25%): ${mb(thetaSz)}  ($thetaSz B)")
            out.appendLine(
                "  Joint-profile                 : ${mb(
                    jpSz,
                )}  ($jpSz B)  (${seg.jointProfile?.profileCount() ?: 0} profiles for $nUsers users)",
            )

            // Queries.
            out.appendLine()
            out.appendLine("QUERY LATENCY (p50 of 100 runs after 10 warmup):")
            out.appendLine(
                "  %-26s %12s %12s %12s %12s %12s   %s".format(
                    "shape",
                    "roaring",
                    "reordered",
                    "bsi",
                    "theta",
                    "joint_prof",
                    "ground-truth → models",
                ),
            )

            for ((label, ast) in queries) {
                val req = QueryRequest(Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), ast, "cardinality")
                val models =
                    listOf(
                        IndexModelKind.ROARING,
                        IndexModelKind.ROARING_REORDERED,
                        IndexModelKind.BSI,
                        IndexModelKind.THETA,
                        IndexModelKind.JOINT_PROFILE,
                    )
                // Sample to get one canonical result + one row of per-model results for printing.
                val sampleResults = mev.evaluate(listOf(seg), req, models)
                val truth = (sampleResults.first { it.model == IndexModelKind.ROARING }.value as Number).toLong()

                val timings =
                    models.associateWith { m ->
                        // Skip timing if the model is unsupported for this query.
                        val supported = sampleResults.first { it.model == m }.supported
                        if (!supported) {
                            -1L
                        } else {
                            timeP50(10, 100) { mev.evaluate(listOf(seg), req, listOf(m)) }
                        }
                    }

                fun fmtTime(ns: Long) = if (ns < 0) "n/a" else us(ns)

                fun fmtVal(m: IndexModelKind): String {
                    val r = sampleResults.first { it.model == m }
                    if (!r.supported) return "—"
                    val v = (r.value as Number).toLong()
                    return if (m == IndexModelKind.THETA) {
                        val err = if (truth > 0) (v - truth) * 100.0 / truth else 0.0
                        "$v (${"%.1f".format(err)}%)"
                    } else {
                        v.toString()
                    }
                }
                val timingRow =
                    "  %-26s %12s %12s %12s %12s %12s".format(
                        label,
                        fmtTime(timings[IndexModelKind.ROARING]!!),
                        fmtTime(timings[IndexModelKind.ROARING_REORDERED]!!),
                        fmtTime(timings[IndexModelKind.BSI]!!),
                        fmtTime(timings[IndexModelKind.THETA]!!),
                        fmtTime(timings[IndexModelKind.JOINT_PROFILE]!!),
                    )
                val valuesPart =
                    "   truth=$truth | R=${fmtVal(IndexModelKind.ROARING)} Rr=${fmtVal(
                        IndexModelKind.ROARING_REORDERED,
                    )} B=${fmtVal(IndexModelKind.BSI)} T=${fmtVal(IndexModelKind.THETA)} JP=${fmtVal(IndexModelKind.JOINT_PROFILE)}"
                out.appendLine(timingRow + valuesPart)
            }

            // On-disk size (full segment dir incl. dims/dicts/metrics/models).
            val io = SegmentIO(c)
            io.write(seg)
            val onDisk =
                Files
                    .walk(tmp)
                    .filter { Files.isRegularFile(it) }
                    .mapToLong { Files.size(it) }
                    .sum()
            out.appendLine()
            out.appendLine("ON-DISK FOOTPRINT (entire segment dir, zstd-compressed): ${mb(onDisk)}  ($onDisk B)")

            // Clean tmp for next scale iteration.
            Files
                .walk(tmp)
                .sorted(Comparator.reverseOrder())
                .forEach { p -> if (p != tmp) Files.deleteIfExists(p) }
        }

        out.appendLine()
        out.appendLine("=".repeat(110))
        println(out)
    }
}

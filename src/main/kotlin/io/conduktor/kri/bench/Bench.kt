package io.conduktor.kri.bench

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.index.SegmentStore
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.HttpServer
import io.conduktor.kri.query.QueryRequest
import java.time.Instant
import java.util.SplittableRandom

/**
 * Standalone in-process benchmark: generates synthetic events, feeds them to SegmentStore
 * directly (bypassing Kafka), then runs a suite of representative queries with timings.
 *
 * Args:
 *   --events N     total events to ingest      (default 20_000_000)
 *   --buckets N    number of hourly segments   (default 6)
 *   --users N      size of distinct user pool  (default 1_000_000)
 *   --paths N      size of distinct path pool  (default 10_000)
 */
fun main(args: Array<String>) {
    val opts = parseOpts(args)
    val nEvents = opts["events"]?.toLong() ?: 20_000_000L
    val nBuckets = opts["buckets"]?.toInt() ?: 6
    val nUsers = opts["users"]?.toInt() ?: 1_000_000
    val nPaths = opts["paths"]?.toInt() ?: 10_000
    val nPartitions = opts["partitions"]?.toInt() ?: 1
    val serve = "serve" in opts || args.contains("--serve")

    println("bench: events=$nEvents buckets=$nBuckets partitions=$nPartitions users=$nUsers paths=$nPaths")

    val cfg = ConfigLoader.parseOnly(BENCH_CFG)
    val store = SegmentStore(cfg)
    val evaluator = Evaluator(cfg)

    val bucketMs = 3_600_000L
    val t0 = Instant.parse("2026-04-24T00:00:00Z").toEpochMilli()
    val tEnd = t0 + nBuckets * bucketMs

    val countries = arrayOf("FR", "DE", "US", "GB", "IT", "ES", "JP", "BR", "IN", "CA")
    //                         FR    DE    US    GB    IT    ES    JP    BR    IN    CA
    val countryW = doubleArrayOf(0.25, 0.15, 0.20, 0.10, 0.06, 0.05, 0.05, 0.05, 0.06, 0.03)
    cumulate(countryW)
    val devices = arrayOf("mobile", "desktop", "tablet")
    val deviceW = doubleArrayOf(0.60, 0.35, 0.05)
    cumulate(deviceW)
    val statuses = intArrayOf(200, 200, 200, 200, 200, 200, 204, 301, 302, 400, 404, 500, 502, 503)

    // ingest
    val rnd = SplittableRandom(42)
    val ingestStart = System.nanoTime()
    var lastReport = ingestStart
    val batch = HashMap<String, Any?>(8)
    for (i in 0L until nEvents) {
        val tsRaw = t0 + (i * bucketMs * nBuckets / nEvents) + rnd.nextLong(1000)
        val ts = if (tsRaw >= tEnd) tEnd - 1 else tsRaw
        val user = rnd.nextInt(nUsers)
        batch.clear()
        batch["userId"] = user
        batch["country"] = countries[pick(rnd.nextDouble(), countryW)]
        batch["device"] = devices[pick(rnd.nextDouble(), deviceW)]
        batch["status"] = statuses[rnd.nextInt(statuses.size)]
        batch["path"] = "/p/${rnd.nextInt(nPaths)}"
        batch["latency"] = rnd.nextInt(2000)
        batch["tsMs"] = ts
        val partition = (user % nPartitions)
        store.segmentFor(partition, Instant.ofEpochMilli(ts)).add(partition, i, batch)

        if ((i and 0x1FFFFFL) == 0L && i > 0) {
            val now = System.nanoTime()
            val elapsedMs = (now - lastReport) / 1_000_000
            val rate = if (elapsedMs > 0) (0x200000L * 1000L / elapsedMs) else 0
            println("  ingested ${fmt(i)} / ${fmt(nEvents)}  (${fmt(rate)} ev/s last window)")
            lastReport = now
        }
    }
    val ingestNs = System.nanoTime() - ingestStart
    val ingestRate = nEvents * 1_000_000_000.0 / ingestNs

    println()
    println("ingest: ${fmt(nEvents)} events in ${"%.2f".format(ingestNs / 1e9)} s  (${fmt(ingestRate.toLong())} ev/s)")

    // freeze
    val freezeStart = System.nanoTime()
    store.forceRollAll()
    val freezeMs = (System.nanoTime() - freezeStart) / 1_000_000
    println("freeze: $freezeMs ms")

    // segment stats
    println()
    println("segments: (${store.allSegments().size} total across $nPartitions partitions × $nBuckets buckets)")
    store.allSegments().take(12).forEach { s ->
        val dimSummary = s.dims.entries.joinToString(", ") { (d, vm) -> "$d=${vm.size}" }
        println(
            "  seg ${s.id} p=${s.partition} [${s.tStart}..${s.tEnd})  records=${fmt(s.recordCount.get())}  dimValues{$dimSummary}",
        )
    }
    if (store.allSegments().size > 12) println("  ... (${store.allSegments().size - 12} more)")

    // memory
    System.gc()
    Thread.sleep(150)
    System.gc()
    val heap = Runtime.getRuntime().let { it.totalMemory() - it.freeMemory() } / 1024 / 1024
    println("heap used post-ingest: $heap MB")

    // queries
    val qStart = Instant.ofEpochMilli(t0)
    val qEnd = Instant.ofEpochMilli(tEnd)
    val segs = store.allSegments()

    val queries =
        listOf(
            Q("all users", "country:FR", "cardinality"),
            Q("FR AND mobile", "country:FR AND device:mobile", "cardinality"),
            Q("FR,DE mobile NOT 5xx", "country:FR,DE AND device:mobile AND NOT status:500,502,503", "cardinality"),
            Q("IN 7 countries", "country:FR,DE,US,GB,IT,ES,JP", "cardinality"),
            Q("NOT 5xx global", "NOT status:500,502,503", "cardinality"),
            Q("mobile AND 404", "device:mobile AND status:404", "cardinality"),
            Q("count all 5xx", "status:500,502,503", "count"),
            Q("distinctUsers (ULL, no filt)", null, "distinctUsers"),
            Q("distinctUsers FR,DE mobile", "country:FR,DE AND device:mobile", "distinctUsers"),
            Q("totalLatencyMs sum", null, "totalLatencyMs"),
            Q("requestCount global", null, "requestCount"),
        )

    println()
    println("queries (warmup=5, samples=30):")
    println("  %-32s %-14s %11s  %8s  %8s  %8s".format("label", "metric", "result", "p50", "p90", "p99"))
    for (q in queries) {
        val ast = FilterParser.parse(q.filter)
        val req = QueryRequest(qStart, qEnd, ast, q.agg)

        repeat(5) { evaluator.evaluate(segs, req) }
        val samples = LongArray(30)
        for (i in samples.indices) {
            val s = System.nanoTime()
            evaluator.evaluate(segs, req)
            samples[i] = System.nanoTime() - s
        }
        samples.sort()
        val p50 = samples[samples.size / 2]
        val p90 = samples[(samples.size * 90 / 100).coerceAtMost(samples.size - 1)]
        val p99 = samples[(samples.size * 99 / 100).coerceAtMost(samples.size - 1)]
        val resp = evaluator.evaluate(segs, req)
        val resultStr =
            when (val r = resp.result) {
                is Long -> fmt(r)
                is Double -> "%.0f".format(r) + (if (resp.approx) "~" else "")
                else -> r.toString()
            }
        println(
            "  %-32s %-14s %11s  %8s  %8s  %8s".format(
                q.label,
                resp.metric,
                resultStr,
                fmtNs(p50),
                fmtNs(p90),
                fmtNs(p99),
            ),
        )
    }

    // comparison vs naive scan
    println()
    val scanAt100k = nEvents.toDouble() / 100_000.0
    val scanAt1m = nEvents.toDouble() / 1_000_000.0
    println(
        "scan reference: scanning ${fmt(nEvents)} records would take ~%.1fs @ 100k msg/s or ~%.1fs @ 1M msg/s, per query."
            .format(scanAt100k, scanAt1m),
    )

    if (serve) {
        println()
        println("serve: starting HTTP on http://localhost:8080  (frontend at http://localhost:8080/)")
        val http = HttpServer(cfg, store, evaluator)
        http.start()
        Runtime.getRuntime().addShutdownHook(
            Thread {
                http.stop()
                evaluator.shutdown()
            },
        )
        Thread.currentThread().join()
    } else {
        evaluator.shutdown()
    }
}

private data class Q(
    val label: String,
    val filter: String?,
    val agg: String,
)

private fun parseOpts(args: Array<String>): Map<String, String> {
    val m = HashMap<String, String>()
    var i = 0
    while (i < args.size) {
        val k = args[i]
        if (k.startsWith("--") && i + 1 < args.size) {
            m[k.removePrefix("--")] = args[i + 1]
            i += 2
        } else {
            i++
        }
    }
    return m
}

private fun cumulate(w: DoubleArray) {
    var s = 0.0
    for (i in w.indices) {
        s += w[i]
        w[i] = s
    }
    val last = w.last()
    if (last > 0.0) for (i in w.indices) w[i] = w[i] / last
}

private fun pick(
    r: Double,
    cdf: DoubleArray,
): Int {
    for (i in cdf.indices) if (r <= cdf[i]) return i
    return cdf.size - 1
}

private fun fmt(n: Long): String {
    if (n < 1000) return n.toString()
    val s = n.toString()
    val sb = StringBuilder()
    var c = 0
    for (i in s.indices.reversed()) {
        sb.append(s[i])
        c++
        if (c == 3 && i > 0) {
            sb.append('_')
            c = 0
        }
    }
    return sb.reverse().toString()
}

private fun fmtNs(ns: Long): String =
    when {
        ns < 10_000L -> "${ns}ns"
        ns < 10_000_000L -> "${ns / 1_000}µs"
        else -> "${ns / 1_000_000}ms"
    }

private val BENCH_CFG =
    """
apiVersion: indexer.conduktor.io/v1
kind: IndexerConfig
metadata: { name: bench }
source: { topic: events, bootstrap: unused:9092, consumerGroup: bench }
segmentation: { strategy: time, bucket: 1h, maxOpenSegments: 64 }
fields:
  userId:  { path: userId,  type: uint32 }
  country: { path: country, type: string }
  device:  { path: device,  type: string }
  status:  { path: status,  type: uint16 }
  path:    { path: path,    type: string }
  latency: { path: latency, type: uint32 }
  tsMs:    { path: tsMs,    type: uint64, unit: ms }
timeField: tsMs
member: { field: userId, encoding: raw_uint32 }
dimensions:
  - { name: country, field: country, encoding: dict, maxCardinality: 300 }
  - { name: device,  field: device,  encoding: dict, maxCardinality: 50 }
  - { name: status,  field: status,  encoding: raw_uint32 }
  - { name: path,    field: path,    encoding: hash32, hashAlgo: murmur3 }
metrics:
  - { name: distinctUsers,  type: ull, field: userId, precision: 12 }
  - { name: requestCount,   type: count }
  - { name: totalLatencyMs, type: sum, field: latency }
storage: { path: /tmp/kri-bench }
query: { maxSegmentsPerQuery: 720, parallelThreshold: 4 }
    """.trimIndent()

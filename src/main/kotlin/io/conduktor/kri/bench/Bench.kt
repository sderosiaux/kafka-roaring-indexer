package io.conduktor.kri.bench

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.index.SegmentStore
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.HttpServer
import io.conduktor.kri.query.QueryRequest
import java.time.Instant
import java.util.SplittableRandom
import kotlin.math.PI
import kotlin.math.cos
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.sqrt

fun main(args: Array<String>) {
    val opts = parseOpts(args)
    val nEvents = opts["events"]?.toLong() ?: 10_000_000L
    val nBuckets = opts["buckets"]?.toInt() ?: 12
    val nUsers = opts["users"]?.toInt() ?: 500_000
    val nPaths = opts["paths"]?.toInt() ?: 20_000
    val nPartitions = opts["partitions"]?.toInt() ?: 4
    val serve = "serve" in opts || args.contains("--serve")

    println("bench: events=$nEvents buckets=$nBuckets partitions=$nPartitions users=$nUsers paths=$nPaths")

    val cfg = ConfigLoader.parseOnly(BENCH_CFG)
    val store = SegmentStore(cfg)
    val evaluator = Evaluator(cfg)

    val bucketMs = 3_600_000L
    val t0 = Instant.parse("2026-04-24T00:00:00Z").toEpochMilli()
    val tEnd = t0 + nBuckets * bucketMs

    // ── Countries (40, power-law α=0.72 → heavy US/IN/BR skew) ────────────
    val countries =
        arrayOf(
            "US",
            "IN",
            "BR",
            "DE",
            "GB",
            "FR",
            "ID",
            "MX",
            "RU",
            "JP",
            "TR",
            "KR",
            "IT",
            "PL",
            "ES",
            "PH",
            "TH",
            "VN",
            "SA",
            "AU",
            "NL",
            "CA",
            "AR",
            "UA",
            "BE",
            "SE",
            "AT",
            "ZA",
            "NG",
            "EG",
            "MY",
            "PT",
            "CZ",
            "RO",
            "PK",
            "BD",
            "CH",
            "IR",
            "DZ",
            "SG",
        )
    val countryW = DoubleArray(countries.size) { i -> 1.0 / (i + 1.0).pow(0.72) }
    cumulate(countryW)

    // mobile affinity [0=desktop … 1=mobile] per country
    val mobilePct =
        mapOf(
            "IN" to 0.88,
            "ID" to 0.85,
            "NG" to 0.83,
            "BD" to 0.82,
            "PH" to 0.80,
            "BR" to 0.73,
            "MX" to 0.70,
            "SA" to 0.68,
            "EG" to 0.67,
            "TR" to 0.65,
            "VN" to 0.64,
            "TH" to 0.62,
            "DZ" to 0.64,
            "IR" to 0.66,
            "PK" to 0.75,
            "AR" to 0.63,
            "MY" to 0.60,
            "ZA" to 0.58,
            "RU" to 0.55,
            "UA" to 0.53,
            "JP" to 0.57,
            "KR" to 0.59,
            "SG" to 0.56,
            "US" to 0.52,
            "GB" to 0.50,
            "FR" to 0.48,
            "IT" to 0.51,
            "ES" to 0.53,
            "AU" to 0.49,
            "CA" to 0.48,
            "PL" to 0.52,
            "PT" to 0.54,
            "CZ" to 0.44,
            "RO" to 0.50,
            "UA" to 0.53,
            "DE" to 0.38,
            "NL" to 0.40,
            "SE" to 0.41,
            "BE" to 0.42,
            "AT" to 0.39,
            "CH" to 0.38,
        )

    // ── OS (correlated with device below) ───────────────────────────────────
    // mobile → Android/iOS, tablet → iOS-heavy, desktop → Windows/macOS/Linux

    // ── Plans (heavy free tier) ─────────────────────────────────────────────
    val plans = arrayOf("free", "basic", "pro", "enterprise")
    val planW = doubleArrayOf(0.60, 0.22, 0.13, 0.05)
    cumulate(planW)

    // ── Status codes (realistic: 200 dominates, errors rare) ────────────────
    val statuses = intArrayOf(200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 429, 500, 502, 503, 504)
    val statusW = doubleArrayOf(0.710, 0.048, 0.058, 0.012, 0.019, 0.033, 0.022, 0.013, 0.008, 0.029, 0.012, 0.013, 0.007, 0.006, 0.010)
    cumulate(statusW)

    // ── Paths  (Zipf α=1.1: top-100 paths get ~60% of traffic) ─────────────
    val pathW = DoubleArray(nPaths) { i -> 1.0 / (i + 1.0).pow(1.1) }
    cumulate(pathW)

    // ── Users (Zipf α=0.75: power-users + long tail of inactives) ───────────
    val userW = DoubleArray(nUsers) { i -> 1.0 / (i + 1.0).pow(0.75) }
    cumulate(userW)

    // ── Ingest ───────────────────────────────────────────────────────────────
    val rnd = SplittableRandom(42)
    val ingestStart = System.nanoTime()
    var lastReport = ingestStart
    val batch = HashMap<String, Any?>(14)

    for (i in 0L until nEvents) {
        // Sinusoidal time: traffic builds up then fades — peaks around bucket 7-8
        val tNorm = i.toDouble() / nEvents
        val tBias = 0.5 - 0.42 * cos(tNorm * 2.0 * PI)
        val ts =
            (t0 + (tBias * nBuckets * bucketMs).toLong() + rnd.nextLong(120_000L))
                .coerceIn(t0, tEnd - 1)

        val user = pickFast(rnd.nextDouble(), userW)
        val countryIdx = pickFast(rnd.nextDouble(), countryW)
        val country = countries[countryIdx]

        val mPct = mobilePct[country] ?: 0.54
        val device =
            when {
                rnd.nextDouble() < mPct -> "mobile"
                rnd.nextDouble() < 0.09 -> "tablet"
                else -> "desktop"
            }
        val os =
            when (device) {
                "mobile" -> if (rnd.nextDouble() < 0.55) "Android" else "iOS"
                "tablet" -> if (rnd.nextDouble() < 0.68) "iOS" else "Android"
                else -> {
                    val r = rnd.nextDouble()
                    when {
                        r < 0.50 -> "Windows"
                        r < 0.76 -> "macOS"
                        r < 0.93 -> "Linux"
                        else -> "Other"
                    }
                }
            }

        val status = statuses[pickFast(rnd.nextDouble(), statusW)]
        val pathIdx = pickFast(rnd.nextDouble(), pathW)
        val plan = plans[pickFast(rnd.nextDouble(), planW)]

        // Bimodal latency: fast (cache hit) vs slow (origin) vs spike
        val latency: Int =
            when {
                status >= 500 -> {
                    // errors are slow: log-normal centered ~2s
                    logNormal(rnd, mu = 7.5, sigma = 0.8).toInt().coerceIn(200, 30_000)
                }
                status in 300..399 -> rnd.nextInt(8) + 1 // redirects are instant
                rnd.nextDouble() < 0.68 -> // cache hit
                    logNormal(rnd, mu = 3.2, sigma = 0.55).toInt().coerceIn(1, 250)
                rnd.nextDouble() < 0.07 -> // p99 spike
                    rnd.nextInt(8_000) + 2_000
                else -> // origin fetch
                    logNormal(rnd, mu = 6.0, sigma = 0.65).toInt().coerceIn(80, 6_000)
            }

        batch.clear()
        batch["userId"] = user
        batch["country"] = country
        batch["device"] = device
        batch["os"] = os
        batch["status"] = status
        batch["path"] = "/p/$pathIdx"
        batch["latency"] = latency
        batch["plan"] = plan
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

    println()
    println("segments: (${store.allSegments().size} total across $nPartitions partitions × $nBuckets buckets)")
    store.allSegments().take(12).forEach { s ->
        val dimSummary = s.dims.entries.joinToString(", ") { (d, vm) -> "$d=${vm.size}" }
        println(
            "  seg ${s.id} p=${s.partition} [${s.tStart}..${s.tEnd})  records=${fmt(s.recordCount.get())}  dimValues{$dimSummary}",
        )
    }
    if (store.allSegments().size > 12) println("  ... (${store.allSegments().size - 12} more)")

    System.gc()
    Thread.sleep(150)
    System.gc()
    val heap = Runtime.getRuntime().let { it.totalMemory() - it.freeMemory() } / 1024 / 1024
    println("heap used post-ingest: $heap MB")

    val qStart = Instant.ofEpochMilli(t0)
    val qEnd = Instant.ofEpochMilli(tEnd)
    val segs = store.allSegments()

    val queries =
        listOf(
            Q("all users", null, "cardinality"),
            Q("US mobile", "country:US AND device:mobile", "cardinality"),
            Q("IN+BR mobile", "country:IN,BR AND device:mobile", "cardinality"),
            Q("pro+enterprise", "plan:pro,enterprise", "cardinality"),
            Q("5xx errors", "status:500,502,503,504", "count"),
            Q("mobile Android NOT free", "device:mobile AND os:Android AND NOT plan:free", "cardinality"),
            Q("top-10 countries", "country:US,IN,BR,DE,GB,FR,ID,MX,RU,JP", "cardinality"),
            Q("NOT errors global", "NOT status:500,502,503,504", "count"),
            Q("distinctUsers (no filt)", null, "distinctUsers"),
            Q("distinctUsers mobile Android", "device:mobile AND os:Android", "distinctUsers"),
            Q("totalLatencyMs all", null, "totalLatencyMs"),
            Q("totalLatencyMs 5xx", "status:500,502,503,504", "totalLatencyMs"),
            Q("requestCount enterprise", "plan:enterprise", "requestCount"),
        )

    println()
    println("queries (warmup=5, samples=30):")
    println("  %-36s %-14s %11s  %8s  %8s  %8s".format("label", "metric", "result", "p50", "p90", "p99"))
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
            "  %-36s %-14s %11s  %8s  %8s  %8s".format(
                q.label,
                resp.metric,
                resultStr,
                fmtNs(p50),
                fmtNs(p90),
                fmtNs(p99),
            ),
        )
    }

    println()
    val scanAt1m = nEvents.toDouble() / 1_000_000.0
    println("scan reference: scanning ${fmt(nEvents)} records @ 1M msg/s = ~%.1fs per query.".format(scanAt1m))

    if (serve) {
        println()
        println("serve: starting HTTP on http://localhost:8080/")
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

private fun logNormal(
    rnd: SplittableRandom,
    mu: Double,
    sigma: Double,
): Double {
    val u1 = rnd.nextDouble().coerceAtLeast(1e-12)
    val u2 = rnd.nextDouble()
    val z = sqrt(-2.0 * ln(u1)) * cos(2.0 * PI * u2)
    return exp(mu + sigma * z).coerceAtLeast(0.0)
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

/** Binary search on CDF — O(log N), required for large distributions. */
private fun pickFast(
    r: Double,
    cdf: DoubleArray,
): Int {
    var lo = 0
    var hi = cdf.size - 1
    while (lo < hi) {
        val mid = (lo + hi) ushr 1
        if (cdf[mid] < r) lo = mid + 1 else hi = mid
    }
    return lo
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

private fun fmt(n: Double): String = fmt(n.toLong())

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
  os:      { path: os,      type: string }
  status:  { path: status,  type: uint16 }
  path:    { path: path,    type: string }
  latency: { path: latency, type: uint32 }
  plan:    { path: plan,    type: string }
  tsMs:    { path: tsMs,    type: uint64, unit: ms }
timeField: tsMs
member: { field: userId, encoding: raw_uint32 }
dimensions:
  - { name: country, field: country, encoding: dict,    maxCardinality: 300 }
  - { name: device,  field: device,  encoding: dict,    maxCardinality: 50  }
  - { name: os,      field: os,      encoding: dict,    maxCardinality: 20  }
  - { name: plan,    field: plan,    encoding: dict,    maxCardinality: 10  }
  - { name: status,  field: status,  encoding: raw_uint32 }
  - { name: path,    field: path,    encoding: hash32,  hashAlgo: murmur3   }
metrics:
  - { name: distinctUsers,  type: ull,   field: userId,  precision: 12 }
  - { name: requestCount,   type: count }
  - { name: totalLatencyMs, type: sum,   field: latency }
storage: { path: /tmp/kri-bench }
query: { maxSegmentsPerQuery: 720, parallelThreshold: 4 }
    """.trimIndent()

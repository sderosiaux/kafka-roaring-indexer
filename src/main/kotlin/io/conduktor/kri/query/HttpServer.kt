package io.conduktor.kri.query

import com.dynatrace.hash4j.distinctcount.UltraLogLog
import com.fasterxml.jackson.databind.ObjectMapper
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.Metric
import io.conduktor.kri.index.DictEncoder
import io.conduktor.kri.index.RawUInt32Encoder
import io.conduktor.kri.index.SegmentStore
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.staticfiles.Location
import org.roaringbitmap.FastAggregation
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.Base64
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class HttpServer(
    private val cfg: IndexerConfig,
    private val store: SegmentStore,
    private val evaluator: Evaluator,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private lateinit var app: Javalin
    private val mapper = ObjectMapper()

    private val httpClient: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build()

    fun start() {
        val bind = cfg.query.http.bind
        val (host, portStr) = bind.split(":").let { it[0] to it[1] }
        val port = portStr.toInt()

        app =
            Javalin
                .create { c ->
                    c.showJavalinBanner = false
                    c.staticFiles.add("/public", Location.CLASSPATH)
                    c.bundledPlugins.enableCors { cors ->
                        cors.addRule { it.anyHost() }
                    }
                }.get("/health") { ctx -> ctx.result("ok") }
                .get("/segments") { ctx -> handleSegments(ctx) }
                .get("/query") { ctx -> handleQuery(ctx) }
                .get("/facets") { ctx -> handleFacets(ctx) }
                .get("/cardinality") { ctx -> handleCardinalityLegacy(ctx) }
                .get("/internal/query") { ctx -> handleInternalQuery(ctx) }
                .get("/nodes") { ctx -> handleNodes(ctx) }
                .start(host, port)
        log.info("HTTP listening on http://{}:{}", host, port)
        if (cfg.query.peers.isNotEmpty()) {
            log.info("Coordinator mode: {} peer(s) configured", cfg.query.peers.size)
        }
    }

    fun stop() {
        if (::app.isInitialized) app.stop()
    }

    // ── Internal endpoint (called by coordinator peers) ──────────────────────

    /**
     * Returns local query result in a format the coordinator can merge.
     * For cardinality: includes base64-encoded RoaringBitmap.
     * For ULL metrics: includes base64-encoded sketch state.
     * For count/sum: scalar only.
     */
    private fun handleInternalQuery(ctx: Context) {
        try {
            val from = parseInstant(ctx.queryParam("from")) ?: Instant.EPOCH
            val to = parseInstant(ctx.queryParam("to")) ?: Instant.now().plusSeconds(1)
            val filter = FilterParser.parse(ctx.queryParam("filter"))
            val agg = ctx.queryParam("agg") ?: "count"
            val partitions = parsePartitions(ctx)
            val segs = store.segmentsOverlapping(from, to, partitions)
            val req = QueryRequest(from, to, filter, agg)
            val resp = evaluator.evaluate(segs, req)

            val out =
                mutableMapOf<String, Any?>(
                    "segmentCount" to resp.segmentCount,
                    "matchedRecords" to resp.matchedRecords,
                    "metric" to resp.metric,
                    "approx" to resp.approx,
                    "result" to resp.result,
                )

            // Cardinality: also return the raw bitmap so coordinator can union correctly.
            if (agg == "cardinality") {
                val bm = evaluator.evalMergedBitmap(segs, filter)
                val buf = ByteArrayOutputStream()
                bm.serialize(DataOutputStream(buf))
                out["bitmap"] = Base64.getEncoder().encodeToString(buf.toByteArray())
            }

            // ULL metric: also return the merged sketch state for coordinator merge.
            val metric = cfg.metrics.firstOrNull { it.name == agg }
            if (metric != null && metric.type == Metric.Type.ULL) {
                val hasFilter = filter !is FilterAst.True
                if (!hasFilter && metric.sliceBy.orEmpty().isEmpty()) {
                    val merged =
                        segs
                            .mapNotNull { it.ullSketches[metric.name]?.get(emptyList()) }
                            .fold(null as UltraLogLog?) { acc, sk ->
                                if (acc == null) UltraLogLog.wrap(sk.state.copyOf()) else UltraLogLog.merge(acc, sk)
                            }
                    if (merged != null) {
                        out["sketch"] = Base64.getEncoder().encodeToString(merged.state)
                    }
                }
            }

            ctx.json(out)
        } catch (e: FilterParseException) {
            ctx.status(400).json(mapOf("error" to "filter parse: ${e.message}"))
        } catch (e: Exception) {
            log.error("internal query failed", e)
            ctx.status(500).json(mapOf("error" to e.message))
        }
    }

    // ── Public query endpoint ────────────────────────────────────────────────

    private fun handleQuery(ctx: Context) {
        if (cfg.query.peers.isEmpty()) {
            handleQueryLocal(ctx)
        } else {
            handleQueryFanout(ctx)
        }
    }

    private fun handleQueryLocal(ctx: Context) {
        try {
            val from = parseInstant(ctx.queryParam("from")) ?: Instant.EPOCH
            val to = parseInstant(ctx.queryParam("to")) ?: Instant.now().plusSeconds(1)
            val filter = FilterParser.parse(ctx.queryParam("filter"))
            val agg = ctx.queryParam("agg") ?: "count"
            val partitions = parsePartitions(ctx)
            val segs = store.segmentsOverlapping(from, to, partitions)
            if (segs.size > cfg.query.maxSegmentsPerQuery) {
                ctx.status(400).json(mapOf("error" to "too many segments: ${segs.size} > ${cfg.query.maxSegmentsPerQuery}"))
                return
            }
            val resp = evaluator.evaluate(segs, QueryRequest(from, to, filter, agg))
            ctx.json(
                mapOf(
                    "segments" to resp.segmentCount,
                    "matched" to resp.matchedRecords,
                    "result" to resp.result,
                    "metric" to resp.metric,
                    "approx" to resp.approx,
                ),
            )
        } catch (e: FilterParseException) {
            ctx.status(400).json(mapOf("error" to "filter parse: ${e.message}"))
        } catch (e: IllegalArgumentException) {
            ctx.status(400).json(mapOf("error" to e.message))
        } catch (e: Exception) {
            log.error("query failed", e)
            ctx.status(500).json(mapOf("error" to e.message))
        }
    }

    private fun handleQueryFanout(ctx: Context) {
        try {
            val qs = ctx.queryString() ?: ""
            val agg = ctx.queryParam("agg") ?: "count"

            // Self: query local segments in-memory.
            val from = parseInstant(ctx.queryParam("from")) ?: Instant.EPOCH
            val to = parseInstant(ctx.queryParam("to")) ?: Instant.now().plusSeconds(1)
            val filter = FilterParser.parse(ctx.queryParam("filter"))
            val partitions = parsePartitions(ctx)
            val segs = store.segmentsOverlapping(from, to, partitions)
            val selfResp = evaluator.evaluate(segs, QueryRequest(from, to, filter, agg))

            // Self bitmap for cardinality merge.
            val selfBitmap = if (agg == "cardinality") evaluator.evalMergedBitmap(segs, filter) else null

            // Peers: call /internal/query in parallel.
            val peerFutures =
                cfg.query.peers.map { peer ->
                    httpClient
                        .sendAsync(
                            HttpRequest
                                .newBuilder(URI("$peer/internal/query?$qs"))
                                .timeout(Duration.ofSeconds(10))
                                .GET()
                                .build(),
                            HttpResponse.BodyHandlers.ofString(),
                        ).thenApply<Map<String, Any?>?> { resp ->
                            if (resp.statusCode() == 200) parseJsonMap(resp.body()) else null
                        }.exceptionally { e ->
                            log.warn("peer {} unreachable: {}", peer, e.message)
                            null
                        }
                }

            val peerResults = peerFutures.map { it.get(12, TimeUnit.SECONDS) }
            val availablePeers = peerResults.filterNotNull()
            val partial = availablePeers.size < cfg.query.peers.size

            val totalSegments = selfResp.segmentCount + availablePeers.sumOf { (it["segmentCount"] as? Number)?.toInt() ?: 0 }
            val totalMatched = selfResp.matchedRecords + availablePeers.sumOf { (it["matchedRecords"] as? Number)?.toLong() ?: 0L }

            val mergedResult: Any? =
                when (agg) {
                    "cardinality" -> {
                        val bitmaps = mutableListOf<RoaringBitmap>()
                        if (selfBitmap != null) bitmaps += selfBitmap
                        availablePeers.forEach { peer ->
                            val b64 = peer["bitmap"] as? String ?: return@forEach
                            val bm = RoaringBitmap()
                            bm.deserialize(java.io.DataInputStream(Base64.getDecoder().decode(b64).inputStream()))
                            bitmaps += bm
                        }
                        if (bitmaps.isEmpty()) 0L else FastAggregation.or(*bitmaps.toTypedArray()).longCardinality.toLong()
                    }
                    "count" -> {
                        val selfVal = selfResp.result as? Long ?: 0L
                        selfVal + availablePeers.sumOf { (it["result"] as? Number)?.toLong() ?: 0L }
                    }
                    else -> {
                        // sum / stored counter metrics: additive.
                        // ULL: merge sketches if available, otherwise sum estimates.
                        val metric = cfg.metrics.firstOrNull { it.name == agg }
                        if (metric != null && metric.type == Metric.Type.ULL) {
                            val sketches =
                                availablePeers.mapNotNull { peer ->
                                    val b64 = peer["sketch"] as? String ?: return@mapNotNull null
                                    UltraLogLog.wrap(Base64.getDecoder().decode(b64))
                                }
                            if (sketches.isNotEmpty()) {
                                val selfSketch = selfResp.result as? Double
                                val merged =
                                    sketches.fold(null as UltraLogLog?) { acc, sk ->
                                        if (acc == null) UltraLogLog.wrap(sk.state.copyOf()) else UltraLogLog.merge(acc, sk)
                                    }
                                val peerEst = merged?.distinctCountEstimate ?: 0.0
                                (selfSketch ?: 0.0) + peerEst
                            } else {
                                // fallback: sum estimates (approximate)
                                val selfVal = (selfResp.result as? Number)?.toDouble() ?: 0.0
                                selfVal + availablePeers.sumOf { (it["result"] as? Number)?.toDouble() ?: 0.0 }
                            }
                        } else {
                            val selfVal = (selfResp.result as? Number)?.toLong() ?: 0L
                            selfVal + availablePeers.sumOf { (it["result"] as? Number)?.toLong() ?: 0L }
                        }
                    }
                }

            val resp =
                mutableMapOf<String, Any?>(
                    "segments" to totalSegments,
                    "matched" to totalMatched,
                    "result" to mergedResult,
                    "metric" to selfResp.metric,
                    "approx" to (selfResp.approx || availablePeers.any { it["approx"] == true }),
                )
            if (partial) resp["partial"] = true

            ctx.json(resp)
        } catch (e: FilterParseException) {
            ctx.status(400).json(mapOf("error" to "filter parse: ${e.message}"))
        } catch (e: IllegalArgumentException) {
            ctx.status(400).json(mapOf("error" to e.message))
        } catch (e: Exception) {
            log.error("fanout query failed", e)
            ctx.status(500).json(mapOf("error" to e.message))
        }
    }

    // ── Facets ───────────────────────────────────────────────────────────────

    private fun handleFacets(ctx: Context) {
        if (cfg.query.peers.isEmpty()) {
            handleFacetsLocal(ctx)
        } else {
            handleFacetsFanout(ctx)
        }
    }

    private fun handleFacetsLocal(ctx: Context) {
        try {
            val from = parseInstant(ctx.queryParam("from")) ?: Instant.EPOCH
            val to = parseInstant(ctx.queryParam("to")) ?: Instant.now().plusSeconds(1)
            val filter = FilterParser.parse(ctx.queryParam("filter"))
            val partitions = parsePartitions(ctx)
            val topN = ctx.queryParam("topN")?.toIntOrNull()?.coerceIn(1, 1000) ?: 20
            val segs = store.segmentsOverlapping(from, to, partitions)

            ctx.json(
                mapOf(
                    "segments" to segs.size,
                    "facets" to computeFacets(segs, filter, topN),
                ),
            )
        } catch (e: FilterParseException) {
            ctx.status(400).json(mapOf("error" to "filter parse: ${e.message}"))
        } catch (e: Exception) {
            log.error("facets failed", e)
            ctx.status(500).json(mapOf("error" to e.message))
        }
    }

    /**
     * Fan-out facets: each peer returns its local counts per (dim, value).
     * Counts are summed across peers. This is exact when the topic is keyed by
     * member (no cross-partition member overlap), approximate otherwise.
     */
    private fun handleFacetsFanout(ctx: Context) {
        try {
            val from = parseInstant(ctx.queryParam("from")) ?: Instant.EPOCH
            val to = parseInstant(ctx.queryParam("to")) ?: Instant.now().plusSeconds(1)
            val filter = FilterParser.parse(ctx.queryParam("filter"))
            val partitions = parsePartitions(ctx)
            val topN = ctx.queryParam("topN")?.toIntOrNull()?.coerceIn(1, 1000) ?: 20
            val segs = store.segmentsOverlapping(from, to, partitions)

            val qs = ctx.queryString() ?: ""
            val peerFutures =
                cfg.query.peers.map { peer ->
                    httpClient
                        .sendAsync(
                            HttpRequest
                                .newBuilder(URI("$peer/facets?$qs"))
                                .timeout(Duration.ofSeconds(10))
                                .GET()
                                .build(),
                            HttpResponse.BodyHandlers.ofString(),
                        ).thenApply<Map<String, Any?>?> { resp ->
                            if (resp.statusCode() == 200) parseJsonMap(resp.body()) else null
                        }.exceptionally { e ->
                            log.warn("peer {} facets unreachable: {}", peer, e.message)
                            null
                        }
                }

            // Merge: sum counts per (dim, value) across all peers.
            val merged = mutableMapOf<String, MutableMap<String, Long>>()

            // Self contribution.
            val selfFacets = computeFacets(segs, filter, 1000)
            selfFacets.forEach { (dim, items) ->
                val dimMap = merged.getOrPut(dim) { mutableMapOf() }
                @Suppress("UNCHECKED_CAST")
                (items as List<Map<String, Any>>).forEach { item ->
                    val v = item["value"] as String
                    dimMap.merge(v, (item["count"] as Number).toLong(), Long::plus)
                }
            }

            // Peer contributions.
            val availablePeers = peerFutures.map { it.get(12, TimeUnit.SECONDS) }.filterNotNull()
            availablePeers.forEach { peer ->
                @Suppress("UNCHECKED_CAST")
                val facets = peer["facets"] as? Map<String, List<Map<String, Any>>> ?: return@forEach
                facets.forEach { (dim, items) ->
                    val dimMap = merged.getOrPut(dim) { mutableMapOf() }
                    items.forEach { item ->
                        val v = item["value"] as String
                        dimMap.merge(v, (item["count"] as Number).toLong(), Long::plus)
                    }
                }
            }

            val partial = availablePeers.size < cfg.query.peers.size
            val totalSegments = segs.size + availablePeers.sumOf { (it["segments"] as? Number)?.toInt() ?: 0 }
            val facetsOut =
                merged.mapValues { (_, dimMap) ->
                    dimMap.entries
                        .sortedByDescending { it.value }
                        .take(topN)
                        .map { (v, c) -> mapOf("value" to v, "count" to c) }
                }

            val resp =
                mutableMapOf<String, Any?>(
                    "segments" to totalSegments,
                    "facets" to facetsOut,
                )
            if (partial) resp["partial"] = true
            ctx.json(resp)
        } catch (e: FilterParseException) {
            ctx.status(400).json(mapOf("error" to "filter parse: ${e.message}"))
        } catch (e: Exception) {
            log.error("facets fanout failed", e)
            ctx.status(500).json(mapOf("error" to e.message))
        }
    }

    // ── Nodes topology ───────────────────────────────────────────────────────

    private fun handleNodes(ctx: Context) {
        val selfSegs = store.allSegments()
        val self =
            mapOf<String, Any>(
                "url" to "self",
                "bind" to cfg.query.http.bind,
                "online" to true,
                "segments" to selfSegs.size,
                "records" to selfSegs.sumOf { it.recordCount.get() },
            )
        if (cfg.query.peers.isEmpty()) {
            ctx.json(listOf(self))
            return
        }
        val futures =
            cfg.query.peers.map { peer ->
                httpClient
                    .sendAsync(
                        HttpRequest
                            .newBuilder(URI("$peer/health"))
                            .timeout(Duration.ofSeconds(2))
                            .GET()
                            .build(),
                        HttpResponse.BodyHandlers.ofString(),
                    ).thenCompose<Map<String, Any>> { health ->
                        if (health.statusCode() != 200) {
                            CompletableFuture.completedFuture(
                                mapOf("url" to peer, "online" to false, "segments" to 0, "records" to 0L),
                            )
                        } else {
                            httpClient
                                .sendAsync(
                                    HttpRequest
                                        .newBuilder(URI("$peer/segments"))
                                        .timeout(Duration.ofSeconds(2))
                                        .GET()
                                        .build(),
                                    HttpResponse.BodyHandlers.ofString(),
                                ).thenApply<Map<String, Any>> { segsResp ->
                                    @Suppress("UNCHECKED_CAST")
                                    val list = mapper.readValue(segsResp.body(), List::class.java) as List<Map<String, Any>>
                                    mapOf(
                                        "url" to peer,
                                        "online" to true,
                                        "segments" to list.size,
                                        "records" to list.sumOf { (it["recordCount"] as? Number)?.toLong() ?: 0L },
                                    )
                                }
                        }
                    }.exceptionally { _ ->
                        mapOf("url" to peer, "online" to false, "segments" to 0, "records" to 0L)
                    }
            }
        ctx.json(listOf(self) + futures.map { it.get(5, TimeUnit.SECONDS) })
    }

    // ── Shared helpers ───────────────────────────────────────────────────────

    private fun computeFacets(
        segs: List<io.conduktor.kri.index.Segment>,
        filter: FilterAst,
        topN: Int,
    ): Map<String, List<Map<String, Any>>> {
        val dimNames = cfg.dimensions.map { it.name }
        return dimNames.associateWith { dim ->
            val totals = mutableMapOf<Int, Long>()
            for (seg in segs) {
                val matched = evaluator.evalFilter(seg, filter)
                val valueMap = seg.dims[dim] ?: continue
                for ((valueId, bm) in valueMap) {
                    val count = RoaringBitmap.andCardinality(matched, bm).toLong()
                    if (count > 0) totals.merge(valueId, count, Long::plus)
                }
            }
            val encoder = segs.firstOrNull()?.dimEncoder(dim)
            totals.entries
                .sortedByDescending { it.value }
                .take(topN)
                .map { (valueId, count) ->
                    val label =
                        when (encoder) {
                            is DictEncoder -> encoder.dict().reverse(valueId) ?: valueId.toString()
                            is RawUInt32Encoder -> valueId.toLong().and(0xFFFFFFFFL).toString()
                            else -> "0x${valueId.toUInt().toString(16)}"
                        }
                    mapOf("value" to label, "count" to count)
                }
        }
    }

    private fun segmentsInfo(): List<Map<String, Any>> =
        store.allSegments().map { s ->
            mapOf(
                "id" to s.id,
                "partition" to s.partition,
                "tStart" to s.tStart.toString(),
                "tEnd" to s.tEnd.toString(),
                "recordCount" to s.recordCount.get(),
                "frozen" to s.frozen,
                "dims" to s.dims.mapValues { it.value.size },
                "offsets" to s.offsets.mapValues { mapOf("first" to it.value.first, "last" to it.value.last) },
            )
        }

    private fun handleSegments(ctx: Context) {
        if (cfg.query.peers.isEmpty()) ctx.json(segmentsInfo()) else handleSegmentsFanout(ctx)
    }

    private fun handleSegmentsFanout(ctx: Context) {
        val local = segmentsInfo()
        val futures =
            cfg.query.peers.map { peer ->
                httpClient
                    .sendAsync(
                        HttpRequest
                            .newBuilder(URI("$peer/segments"))
                            .timeout(Duration.ofSeconds(5))
                            .GET()
                            .build(),
                        HttpResponse.BodyHandlers.ofString(),
                    ).thenApply { resp ->
                        if (resp.statusCode() == 200) {
                            @Suppress("UNCHECKED_CAST")
                            mapper.readValue(resp.body(), List::class.java) as List<Map<String, Any>>
                        } else {
                            emptyList()
                        }
                    }.exceptionally { emptyList() }
            }
        ctx.json(local + futures.flatMap { it.get(6, TimeUnit.SECONDS) })
    }

    private fun handleCardinalityLegacy(ctx: Context) {
        val dim = ctx.queryParam("dim") ?: return ctx.status(400).json(mapOf("error" to "dim required")).let {}
        val value = ctx.queryParam("value") ?: return ctx.status(400).json(mapOf("error" to "value required")).let {}
        val segs = store.allSegments()
        val total =
            segs.sumOf { seg ->
                val enc = seg.dimEncoder(dim) ?: return@sumOf 0L
                val id =
                    when (enc) {
                        is DictEncoder -> enc.dict().lookup(value) ?: return@sumOf 0L
                        is RawUInt32Encoder ->
                            value.toLongOrNull()?.takeIf { it in 0..0xFFFF_FFFFL }?.toInt()
                                ?: return@sumOf 0L
                        else ->
                            when (val r = enc.encode(value)) {
                                is io.conduktor.kri.index.EncodeResult.Ok -> r.id
                                else -> return@sumOf 0L
                            }
                    }
                seg.dimValueBitmap(dim, id)?.longCardinality?.toLong() ?: 0L
            }
        ctx.json(mapOf("dim" to dim, "value" to value, "cardinality" to total))
    }

    private fun parsePartitions(ctx: Context): Set<Int>? =
        ctx
            .queryParam("partitions")
            ?.split(",")
            ?.mapNotNull { it.trim().toIntOrNull() }
            ?.toSet()
            ?.ifEmpty { null }

    private fun parseInstant(s: String?): Instant? {
        if (s.isNullOrBlank()) return null
        return try {
            Instant.parse(s)
        } catch (e: DateTimeParseException) {
            runCatching { Instant.parse("${s}Z") }.getOrNull()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseJsonMap(body: String): Map<String, Any?> = mapper.readValue(body, Map::class.java) as Map<String, Any?>
}

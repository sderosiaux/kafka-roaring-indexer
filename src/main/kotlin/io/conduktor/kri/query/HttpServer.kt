package io.conduktor.kri.query

import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.index.DictEncoder
import io.conduktor.kri.index.RawUInt32Encoder
import io.conduktor.kri.index.SegmentStore
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.staticfiles.Location
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.format.DateTimeParseException

class HttpServer(
    private val cfg: IndexerConfig,
    private val store: SegmentStore,
    private val evaluator: Evaluator,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private lateinit var app: Javalin

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
                .get("/segments") { ctx -> ctx.json(segmentsInfo()) }
                .get("/query") { ctx -> handleQuery(ctx) }
                .get("/facets") { ctx -> handleFacets(ctx) }
                .get("/cardinality") { ctx -> handleCardinalityLegacy(ctx) }
                .start(host, port)
        log.info("HTTP listening on http://{}:{}", host, port)
    }

    fun stop() {
        if (::app.isInitialized) app.stop()
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

    private fun parsePartitions(ctx: Context): Set<Int>? =
        ctx
            .queryParam("partitions")
            ?.split(",")
            ?.mapNotNull { it.trim().toIntOrNull() }
            ?.toSet()
            ?.ifEmpty { null }

    private fun handleQuery(ctx: Context) {
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

    private fun handleFacets(ctx: Context) {
        try {
            val from = parseInstant(ctx.queryParam("from")) ?: Instant.EPOCH
            val to = parseInstant(ctx.queryParam("to")) ?: Instant.now().plusSeconds(1)
            val filter = FilterParser.parse(ctx.queryParam("filter"))
            val partitions = parsePartitions(ctx)
            val topN = ctx.queryParam("topN")?.toIntOrNull()?.coerceIn(1, 1000) ?: 20
            val segs = store.segmentsOverlapping(from, to, partitions)

            // For each dim: sum per-value cardinality across segments (filtered by matched bitmap).
            val dimNames = cfg.dimensions.map { it.name }
            val facets = mutableMapOf<String, List<Map<String, Any>>>()

            for (dim in dimNames) {
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
                val topValues =
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
                facets[dim] = topValues
            }

            ctx.json(
                mapOf(
                    "segments" to segs.size,
                    "facets" to facets,
                ),
            )
        } catch (e: FilterParseException) {
            ctx.status(400).json(mapOf("error" to "filter parse: ${e.message}"))
        } catch (e: Exception) {
            log.error("facets failed", e)
            ctx.status(500).json(mapOf("error" to e.message))
        }
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
                        is io.conduktor.kri.index.DictEncoder -> enc.dict().lookup(value) ?: return@sumOf 0L
                        is io.conduktor.kri.index.RawUInt32Encoder ->
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

    private fun parseInstant(s: String?): Instant? {
        if (s.isNullOrBlank()) return null
        return try {
            Instant.parse(s)
        } catch (e: DateTimeParseException) {
            runCatching { Instant.parse("${s}Z") }.getOrNull()
        }
    }
}

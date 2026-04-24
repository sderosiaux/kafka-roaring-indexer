package io.conduktor.kri.query

import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.index.SegmentStore
import io.javalin.Javalin
import io.javalin.http.Context
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
                .create { c -> c.showJavalinBanner = false }
                .get("/health") { ctx -> ctx.result("ok") }
                .get("/segments") { ctx -> ctx.json(segmentsInfo()) }
                .get("/query") { ctx -> handleQuery(ctx) }
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
                "tStart" to s.tStart.toString(),
                "tEnd" to s.tEnd.toString(),
                "recordCount" to s.recordCount.get(),
                "frozen" to s.frozen,
                "dims" to s.dims.mapValues { it.value.size },
                "offsets" to s.offsets.mapValues { mapOf("first" to it.value.first, "last" to it.value.last) },
            )
        }

    private fun handleQuery(ctx: Context) {
        try {
            val from = parseInstant(ctx.queryParam("from")) ?: Instant.EPOCH
            val to = parseInstant(ctx.queryParam("to")) ?: Instant.now().plusSeconds(1)
            val filter = FilterParser.parse(ctx.queryParam("filter"))
            val agg = ctx.queryParam("agg") ?: "count"
            val segs = store.segmentsOverlapping(from, to)
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
            // Tolerant: fall back to adding Z suffix if partial.
            runCatching { Instant.parse("${s}Z") }.getOrNull()
        }
    }
}

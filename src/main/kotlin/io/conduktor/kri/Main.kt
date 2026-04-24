package io.conduktor.kri

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.index.SegmentStore
import io.conduktor.kri.ingest.IndexerConsumer
import io.conduktor.kri.persistence.SegmentIO
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.HttpServer
import io.conduktor.kri.schema.SchemaRegistryProbe
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger("kri.Main")
    val configPath =
        parseConfigArg(args) ?: run {
            System.err.println("Usage: kri --config <path-to-yaml>")
            exitProcess(2)
        }

    log.info("Loading config from {}", configPath)
    val cfg =
        try {
            ConfigLoader.loadAndValidate(configPath)
        } catch (e: Exception) {
            System.err.println("Config invalid:\n${e.message}")
            exitProcess(3)
        }
    log.info("Loaded config: {}", cfg.metadata.name)

    when (val r = SchemaRegistryProbe(cfg).probe()) {
        SchemaRegistryProbe.ProbeResult.Ok -> log.info("Schema Registry probe: OK")
        is SchemaRegistryProbe.ProbeResult.Invalid -> {
            System.err.println("Schema Registry probe failed:")
            r.errors.forEach { System.err.println("  - $it") }
            exitProcess(4)
        }
        is SchemaRegistryProbe.ProbeResult.Skipped -> log.info("Schema Registry probe skipped: {}", r.reason)
    }

    val store = SegmentStore(cfg)
    val segmentIO = if (cfg.storage.path.isNotBlank()) SegmentIO(cfg) else null
    segmentIO?.loadAll(store, AtomicLong(0))

    val evaluator = Evaluator(cfg)
    val http = HttpServer(cfg, store, evaluator).also { it.start() }
    val consumer = IndexerConsumer(cfg, store, segmentIO).also { it.start() }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutdown initiated")
            runCatching { consumer.stop() }
            runCatching { http.stop() }
        },
    )

    Thread.currentThread().join()
}

private fun parseConfigArg(args: Array<String>): Path? {
    var i = 0
    while (i < args.size) {
        if ((args[i] == "--config" || args[i] == "-c") && i + 1 < args.size) {
            return Path.of(args[i + 1])
        }
        i++
    }
    return System.getenv("KRI_CONFIG")?.let { Path.of(it) }
}

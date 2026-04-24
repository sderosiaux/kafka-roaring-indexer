package io.conduktor.kri.persistence

import io.conduktor.kri.index.Segment
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Runs segment.write() on a dedicated thread so Kafka's max.poll.interval.ms
 * is not violated during slow I/O. Callers must still await the future before
 * committing offsets to preserve the persist-before-commit invariant.
 */
class AsyncSegmentWriter(
    private val segmentIO: SegmentIO,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val executor =
        Executors.newSingleThreadExecutor { r ->
            Thread(r, "kri-segment-writer").also { it.isDaemon = true }
        }

    fun submit(seg: Segment): CompletableFuture<Unit> =
        CompletableFuture
            .supplyAsync({ segmentIO.write(seg) }, executor)
            .exceptionally { t ->
                log.error("Async segment write failed for id={}: {}", seg.id, t.message, t)
                null
            }.thenApply { }

    fun shutdown() {
        executor.shutdown()
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            log.warn("AsyncSegmentWriter did not terminate within 30s")
        }
    }
}

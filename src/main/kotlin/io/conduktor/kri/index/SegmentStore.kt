package io.conduktor.kri.index

import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.parseDuration
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Owns the set of live (open) and frozen segments in memory.
 * Handles time-bucket alignment and rollover.
 */
class SegmentStore(
    private val cfg: IndexerConfig,
    private val bucket: Duration = parseDuration(cfg.segmentation.bucket),
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val nextId = AtomicLong(0)

    /** bucketStart (epoch ms aligned) → Segment. Open buckets only. */
    private val open = ConcurrentHashMap<Long, Segment>()

    /** All frozen segments in memory (loaded or recent). */
    private val frozen = ConcurrentHashMap<Long, Segment>()

    fun bucketStart(ts: Instant): Instant {
        if (cfg.segmentation.alignment == io.conduktor.kri.config.Segmentation.Alignment.NONE) {
            return ts
        }
        val millis = ts.toEpochMilli()
        val bucketMs = bucket.toMillis()
        val alignedMs = millis - (millis % bucketMs)
        return Instant.ofEpochMilli(alignedMs)
    }

    fun segmentFor(ts: Instant): Segment {
        val start = bucketStart(ts)
        val key = start.toEpochMilli()
        return open.computeIfAbsent(key) { k ->
            val end = Instant.ofEpochMilli(k).plus(bucket)
            val seg = Segment.create(nextId.getAndIncrement(), Instant.ofEpochMilli(k), end, cfg)
            log.info("Opened segment id={} [{} .. {})", seg.id, seg.tStart, seg.tEnd)
            seg
        }
    }

    fun maybeRollover(
        now: Instant = Instant.now(),
        onRoll: (Segment) -> Unit = {},
    ) {
        val cutoff = bucketStart(now).minus(bucket.multipliedBy((cfg.segmentation.maxOpenSegments - 1).toLong()))
        val toRoll = open.entries.filter { it.value.tStart.isBefore(cutoff) }
        toRoll.forEach { (k, seg) ->
            open.remove(k)
            seg.freeze()
            frozen[seg.id] = seg
            log.info("Rolled segment id={} records={} [{} .. {})", seg.id, seg.recordCount.get(), seg.tStart, seg.tEnd)
            onRoll(seg)
        }
    }

    fun allSegments(): List<Segment> = (frozen.values + open.values).sortedBy { it.tStart }

    fun segmentsOverlapping(
        from: Instant,
        to: Instant,
    ): List<Segment> = allSegments().filter { it.tStart.isBefore(to) && it.tEnd.isAfter(from) }

    fun forceRollAll(onRoll: (Segment) -> Unit = {}) {
        val all = open.values.toList()
        open.clear()
        all.forEach {
            it.freeze()
            frozen[it.id] = it
            onRoll(it)
        }
    }

    fun registerFrozen(seg: Segment) {
        frozen[seg.id] = seg
    }

    fun size(): Int = open.size + frozen.size
}

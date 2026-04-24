package io.conduktor.kri.index

import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.parseDuration
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Owns open + frozen segments, keyed by (partition, bucketStart).
 * One segment covers one partition × one time bucket. Cross-partition queries
 * fan out to all matching segments and union the bitmaps.
 */
class SegmentStore(
    private val cfg: IndexerConfig,
    private val bucket: Duration = parseDuration(cfg.segmentation.bucket),
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val nextId = AtomicLong(0)

    data class PartBucket(
        val partition: Int,
        val bucketStartMs: Long,
    )

    /** (partition, bucketStart) → Segment. Open buckets only. */
    private val open = ConcurrentHashMap<PartBucket, Segment>()

    /** segmentId → frozen Segment. */
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

    fun segmentFor(
        partition: Int,
        ts: Instant,
    ): Segment {
        val start = bucketStart(ts)
        val key = PartBucket(partition, start.toEpochMilli())
        return open.computeIfAbsent(key) { k ->
            val end = Instant.ofEpochMilli(k.bucketStartMs).plus(bucket)
            val seg = Segment.create(nextId.getAndIncrement(), k.partition, Instant.ofEpochMilli(k.bucketStartMs), end, cfg)
            log.info("Opened segment id={} partition={} [{} .. {})", seg.id, seg.partition, seg.tStart, seg.tEnd)
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
            log.info(
                "Rolled segment id={} partition={} records={} [{} .. {})",
                seg.id,
                seg.partition,
                seg.recordCount.get(),
                seg.tStart,
                seg.tEnd,
            )
            onRoll(seg)
        }
    }

    fun allSegments(): List<Segment> = (frozen.values + open.values).sortedWith(compareBy({ it.tStart }, { it.partition }))

    fun segmentsOverlapping(
        from: Instant,
        to: Instant,
        partitions: Set<Int>? = null,
    ): List<Segment> =
        allSegments().filter { s ->
            s.tStart.isBefore(to) &&
                s.tEnd.isAfter(from) &&
                (partitions == null || s.partition in partitions)
        }

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
        if (seg.id >= nextId.get()) nextId.set(seg.id + 1)
    }

    fun unregisterFrozen(id: Long) {
        frozen.remove(id)
    }

    fun size(): Int = open.size + frozen.size
}

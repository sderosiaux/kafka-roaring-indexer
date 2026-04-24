package io.conduktor.kri.index

import io.conduktor.kri.config.ConfigLoader
import io.conduktor.kri.query.Evaluator
import io.conduktor.kri.query.FilterParser
import io.conduktor.kri.query.QueryRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ConcurrentQueryTest {
    private val cfg =
        ConfigLoader.parseOnly(
            """
            apiVersion: indexer.conduktor.io/v1
            kind: IndexerConfig
            metadata: { name: concurrent-test }
            source: { topic: t, bootstrap: b:9092, consumerGroup: g }
            segmentation: { strategy: time, bucket: 1h }
            fields:
              userId:  { path: userId,  type: uint32 }
              country: { path: country, type: string }
            member: { field: userId, encoding: raw_uint32 }
            dimensions:
              - { name: country, field: country, encoding: dict }
            storage: { path: /tmp/kri-conc }
            query: { parallelism: 4, parallelThreshold: 2 }
            """.trimIndent(),
        )

    @Test
    fun `concurrent writes do not corrupt reads on open segment`() {
        val seg = Segment.create(0, 0, Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), cfg)
        val ev = Evaluator(cfg)
        val writers = 4
        val recordsPerWriter = 50_000
        val writerPool = Executors.newFixedThreadPool(writers)
        val readerPool = Executors.newFixedThreadPool(2)
        val start = CountDownLatch(1)
        val stop = AtomicBoolean(false)
        val readFailures =
            java.util.concurrent.atomic
                .AtomicInteger(0)
        val readCount =
            java.util.concurrent.atomic
                .AtomicInteger(0)

        repeat(writers) { w ->
            writerPool.submit {
                start.await()
                val base = w.toLong() * recordsPerWriter
                for (i in 0 until recordsPerWriter) {
                    val userId = (base + i)
                    val country = if (i % 2 == 0) "FR" else "DE"
                    seg.add(0, base + i, mapOf("userId" to userId, "country" to country))
                }
            }
        }

        repeat(2) {
            readerPool.submit {
                start.await()
                while (!stop.get()) {
                    try {
                        val r =
                            ev.evaluate(
                                listOf(seg),
                                QueryRequest(
                                    Instant.EPOCH,
                                    Instant.EPOCH.plusSeconds(3600),
                                    FilterParser.parse("country:FR"),
                                    "cardinality",
                                ),
                            )
                        // Any non-negative result is OK; correctness check is no exception + bitmap validity.
                        val v = r.result as Long
                        check(v >= 0)
                        readCount.incrementAndGet()
                    } catch (t: Throwable) {
                        readFailures.incrementAndGet()
                    }
                }
            }
        }

        start.countDown()
        writerPool.shutdown()
        writerPool.awaitTermination(30, TimeUnit.SECONDS)
        stop.set(true)
        readerPool.shutdown()
        readerPool.awaitTermination(5, TimeUnit.SECONDS)
        ev.shutdown()

        assertThat(readFailures.get()).`as`("no reader failures").isEqualTo(0)
        assertThat(readCount.get()).`as`("some reads happened").isGreaterThan(10)

        // Final consistency: after all writes, cardinality should equal total distinct users with country=FR.
        // Each writer contributes recordsPerWriter/2 FR users (even indices), all with distinct userIds.
        val expectedFr = writers.toLong() * (recordsPerWriter / 2)
        val finalResp =
            ev.evaluate(
                listOf(seg),
                QueryRequest(Instant.EPOCH, Instant.EPOCH.plusSeconds(3600), FilterParser.parse("country:FR"), "cardinality"),
            )
        assertThat(finalResp.result as Long).isEqualTo(expectedFr)
    }

    @Test
    fun `parallel path matches sequential path`() {
        val seqCfg =
            ConfigLoader.parseOnly(
                """
                apiVersion: indexer.conduktor.io/v1
                kind: IndexerConfig
                metadata: { name: seq }
                source: { topic: t, bootstrap: b:9092, consumerGroup: g }
                segmentation: { strategy: time, bucket: 1h }
                fields:
                  userId:  { path: userId,  type: uint32 }
                  country: { path: country, type: string }
                member: { field: userId, encoding: raw_uint32 }
                dimensions:
                  - { name: country, field: country, encoding: dict }
                storage: { path: /tmp/kri-seq }
                query: { parallelism: 1 }
                """.trimIndent(),
            )
        val parCfg = seqCfg.copy(query = seqCfg.query.copy(parallelism = 4, parallelThreshold = 1))

        val segs =
            (0 until 16).map { i ->
                val seg =
                    Segment.create(
                        i.toLong(),
                        0,
                        Instant.EPOCH.plusSeconds(i * 3600L),
                        Instant.EPOCH.plusSeconds((i + 1) * 3600L),
                        seqCfg,
                    )
                repeat(100) { k ->
                    seg.add(0, k.toLong(), mapOf("userId" to (i * 100L + k), "country" to if (k % 3 == 0) "FR" else "DE"))
                }
                seg.freeze()
                seg
            }

        val seqEv = Evaluator(seqCfg)
        val parEv = Evaluator(parCfg)
        val req = QueryRequest(Instant.EPOCH, Instant.EPOCH.plusSeconds(20 * 3600L), FilterParser.parse("country:FR"), "cardinality")
        val seq = seqEv.evaluate(segs, req)
        val par = parEv.evaluate(segs, req)
        assertThat(par.result).isEqualTo(seq.result)
        assertThat(par.matchedRecords).isEqualTo(seq.matchedRecords)
        parEv.shutdown()
        seqEv.shutdown()
    }
}

package io.conduktor.kri.ingest

import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.SchemaRegistry
import io.conduktor.kri.index.Segment
import io.conduktor.kri.index.SegmentStore
import io.conduktor.kri.persistence.SegmentIO
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

class IndexerConsumer(
    private val cfg: IndexerConfig,
    private val store: SegmentStore,
    private val segmentIO: SegmentIO?,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val decoder = PayloadDecoder(cfg)
    private val extractor = RecordExtractor(cfg)
    private val running = AtomicBoolean(false)
    private var consumer: KafkaConsumer<ByteArray, Any?>? = null
    private var thread: Thread? = null

    fun start() {
        if (!running.compareAndSet(false, true)) return
        val props = buildProps()
        @Suppress("UNCHECKED_CAST")
        consumer = KafkaConsumer<ByteArray, Any?>(props) as KafkaConsumer<ByteArray, Any?>
        consumer!!.subscribe(listOf(cfg.source.topic))
        thread =
            Thread({ runLoop() }, "kri-consumer").also {
                it.isDaemon = false
                it.start()
            }
        log.info("Consumer started topic={} group={}", cfg.source.topic, cfg.source.consumerGroup)
    }

    fun stop() {
        if (!running.compareAndSet(true, false)) return
        consumer?.wakeup()
        thread?.join(5000)
        log.info("Consumer stopped")
    }

    private fun runLoop() {
        val c = consumer ?: return
        val pending = HashMap<TopicPartition, OffsetAndMetadata>()
        try {
            while (running.get()) {
                val records = c.poll(Duration.ofMillis(500))
                if (records.isEmpty) {
                    store.maybeRollover(onRoll = { seg -> persistAndAck(seg, c, pending) })
                    continue
                }
                for (rec in records) {
                    val payload =
                        try {
                            decoder.decode(rec.value())
                        } catch (e: Exception) {
                            log.warn("decode failed p={} o={}: {}", rec.partition(), rec.offset(), e.message)
                            continue
                        }
                    val fields = extractor.extract(payload)
                    val ts = resolveTimestamp(fields, rec.timestamp())
                    val seg = store.segmentFor(rec.partition(), ts)
                    when (val r = seg.add(rec.partition(), rec.offset(), fields)) {
                        Segment.AddResult.Ok -> {}
                        is Segment.AddResult.Overflow -> log.debug("dim overflow {}", r.dims)
                        Segment.AddResult.MemberMissing -> log.debug("member missing p={} o={}", rec.partition(), rec.offset())
                        Segment.AddResult.RejectedFrozen ->
                            log.warn(
                                "late record to frozen segment p={} o={}",
                                rec.partition(),
                                rec.offset(),
                            )
                    }
                    val tp = TopicPartition(rec.topic(), rec.partition())
                    pending[tp] = OffsetAndMetadata(rec.offset() + 1)
                }
                store.maybeRollover(onRoll = { seg -> persistAndAck(seg, c, pending) })
            }
        } catch (_: org.apache.kafka.common.errors.WakeupException) {
            // expected on shutdown
        } finally {
            runCatching { store.forceRollAll(onRoll = { seg -> persistAndAck(seg, c, pending) }) }
            runCatching { c.commitSync(pending) }
            runCatching { c.close(Duration.ofSeconds(5)) }
        }
    }

    private fun persistAndAck(
        seg: Segment,
        c: KafkaConsumer<*, *>,
        pending: MutableMap<TopicPartition, OffsetAndMetadata>,
    ) {
        // M10 invariant: persist first, THEN commit offsets for partitions covered.
        segmentIO?.write(seg)
        val covered = seg.offsets.keys.map { p -> TopicPartition(cfg.source.topic, p) }
        val commit = pending.filter { it.key in covered }
        if (commit.isNotEmpty()) {
            runCatching { c.commitSync(commit) }.onFailure { log.warn("commit failed: {}", it.message) }
        }
    }

    private fun resolveTimestamp(
        fields: Map<String, Any?>,
        kafkaTsMs: Long,
    ): Instant {
        val tf = cfg.timeField ?: return Instant.ofEpochMilli(kafkaTsMs)
        val v = fields[tf] ?: return Instant.ofEpochMilli(kafkaTsMs)
        val unit = cfg.fields[tf]?.unit
        val ms =
            when (v) {
                is Number ->
                    when (unit) {
                        io.conduktor.kri.config.FieldSpec.TimeUnitSpec.S -> v.toLong() * 1000
                        io.conduktor.kri.config.FieldSpec.TimeUnitSpec.US -> v.toLong() / 1000
                        io.conduktor.kri.config.FieldSpec.TimeUnitSpec.NS -> v.toLong() / 1_000_000
                        io.conduktor.kri.config.FieldSpec.TimeUnitSpec.MS, null -> v.toLong()
                    }
                is String -> runCatching { Instant.parse(v).toEpochMilli() }.getOrNull() ?: kafkaTsMs
                else -> kafkaTsMs
            }
        return Instant.ofEpochMilli(ms)
    }

    private fun buildProps(): Properties {
        val p = Properties()
        p[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = cfg.source.bootstrap
        p[ConsumerConfig.GROUP_ID_CONFIG] = cfg.source.consumerGroup
        p[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] =
            when {
                cfg.source.startOffset == "earliest" -> "earliest"
                else -> "latest"
            }
        p[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        p[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java.name
        val fmt = cfg.source.schemaRegistry?.format
        when (fmt) {
            SchemaRegistry.Format.AVRO -> {
                p[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
                    "io.confluent.kafka.serializers.KafkaAvroDeserializer"
                p["schema.registry.url"] = cfg.source.schemaRegistry!!.url
                p["specific.avro.reader"] = false
            }
            else -> {
                p[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java.name
            }
        }
        p[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 5000
        return p
    }
}

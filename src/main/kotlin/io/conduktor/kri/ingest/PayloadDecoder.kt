package io.conduktor.kri.ingest

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.SchemaRegistry

/**
 * Decodes raw Kafka value bytes / deserialized objects to a tree-like payload
 * that RecordExtractor understands.
 *
 * For Avro: the Confluent deserializer already returned a GenericRecord.
 * For JSON / json-schema: we return a JsonNode.
 * For Map inputs (tests, pre-decoded): pass through.
 */
class PayloadDecoder(
    cfg: IndexerConfig,
) {
    private val format = cfg.source.schemaRegistry?.format ?: SchemaRegistry.Format.JSON
    private val mapper = ObjectMapper()

    fun decode(raw: Any?): Any? {
        if (raw == null) return null
        return when (raw) {
            is Map<*, *>, is JsonNode -> raw
            is org.apache.avro.generic.GenericRecord -> raw
            is ByteArray ->
                when (format) {
                    SchemaRegistry.Format.JSON, SchemaRegistry.Format.JSON_SCHEMA -> mapper.readTree(raw)
                    else -> mapper.readTree(raw)
                }
            is String -> mapper.readTree(raw)
            else -> raw
        }
    }
}

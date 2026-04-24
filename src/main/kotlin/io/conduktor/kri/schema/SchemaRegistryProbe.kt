package io.conduktor.kri.schema

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.conduktor.kri.config.FieldSpec
import io.conduktor.kri.config.IndexerConfig
import io.conduktor.kri.config.SchemaRegistry
import org.slf4j.LoggerFactory
import java.net.HttpURLConnection
import java.net.URI

/**
 * Best-effort check that declared fields exist in the Schema Registry's registered schema.
 * Skipped if no Schema Registry is configured or format != avro/json-schema.
 */
class SchemaRegistryProbe(
    private val cfg: IndexerConfig,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()

    fun probe(): ProbeResult {
        val sr = cfg.source.schemaRegistry ?: return ProbeResult.Skipped("no schemaRegistry configured")
        return try {
            val subject = "${cfg.source.topic}-value"
            val uri = URI.create("${sr.url.trimEnd('/')}/subjects/$subject/versions/latest")
            val conn = uri.toURL().openConnection() as HttpURLConnection
            conn.connectTimeout = 2000
            conn.readTimeout = 2000
            conn.requestMethod = "GET"
            if (conn.responseCode != 200) {
                return ProbeResult.Skipped("schema registry returned ${conn.responseCode}")
            }
            val body = conn.inputStream.bufferedReader().use { it.readText() }
            val envelope = mapper.readTree(body)
            val schemaStr = envelope.get("schema")?.asText() ?: return ProbeResult.Skipped("no 'schema' in envelope")
            when (sr.format) {
                SchemaRegistry.Format.AVRO -> validateAvro(schemaStr)
                SchemaRegistry.Format.JSON_SCHEMA -> validateJsonSchema(schemaStr)
                SchemaRegistry.Format.JSON, SchemaRegistry.Format.PROTOBUF ->
                    ProbeResult.Skipped(
                        "format ${sr.format} not statically validated",
                    )
            }
        } catch (e: Exception) {
            ProbeResult.Skipped("probe failed: ${e.message}")
        }
    }

    private fun validateAvro(schemaStr: String): ProbeResult {
        val schema =
            org.apache.avro.Schema
                .Parser()
                .parse(schemaStr)
        val errors = mutableListOf<String>()
        cfg.fields.forEach { (name, spec) ->
            val resolved = resolveAvroPath(schema, spec.path.split('.').filter { it.isNotEmpty() })
            if (resolved == null) {
                errors += "field '$name' path '${spec.path}' not found in Avro schema"
            } else if (!avroCompat(resolved, spec.type)) {
                errors += "field '$name' type mismatch: avro=${resolved.type} declared=${spec.type}"
            }
        }
        return if (errors.isEmpty()) ProbeResult.Ok else ProbeResult.Invalid(errors)
    }

    private fun resolveAvroPath(
        schema: org.apache.avro.Schema,
        parts: List<String>,
    ): org.apache.avro.Schema? {
        var cur: org.apache.avro.Schema? = schema
        for (p in parts) {
            val s = cur ?: return null
            val record =
                if (s.type ==
                    org.apache.avro.Schema.Type.UNION
                ) {
                    s.types.firstOrNull { it.type == org.apache.avro.Schema.Type.RECORD }
                } else {
                    s
                }
            if (record?.type != org.apache.avro.Schema.Type.RECORD) return null
            cur = record.getField(p)?.schema() ?: return null
        }
        return cur
    }

    private fun avroCompat(
        s: org.apache.avro.Schema,
        t: FieldSpec.FieldType,
    ): Boolean {
        val flat =
            if (s.type ==
                org.apache.avro.Schema.Type.UNION
            ) {
                s.types.firstOrNull { it.type != org.apache.avro.Schema.Type.NULL }?.type
            } else {
                s.type
            }
        return when (t) {
            FieldSpec.FieldType.STRING -> flat == org.apache.avro.Schema.Type.STRING || flat == org.apache.avro.Schema.Type.ENUM
            FieldSpec.FieldType.BYTES -> flat == org.apache.avro.Schema.Type.BYTES
            FieldSpec.FieldType.BOOL -> flat == org.apache.avro.Schema.Type.BOOLEAN
            FieldSpec.FieldType.FLOAT -> flat == org.apache.avro.Schema.Type.FLOAT
            FieldSpec.FieldType.DOUBLE -> flat == org.apache.avro.Schema.Type.DOUBLE || flat == org.apache.avro.Schema.Type.FLOAT
            FieldSpec.FieldType.UINT8, FieldSpec.FieldType.UINT16,
            FieldSpec.FieldType.UINT32, FieldSpec.FieldType.INT32,
            ->
                flat == org.apache.avro.Schema.Type.INT ||
                    flat == org.apache.avro.Schema.Type.LONG
            FieldSpec.FieldType.UINT64, FieldSpec.FieldType.INT64 ->
                flat == org.apache.avro.Schema.Type.LONG ||
                    flat == org.apache.avro.Schema.Type.INT
        }
    }

    private fun validateJsonSchema(schemaStr: String): ProbeResult {
        val root = mapper.readTree(schemaStr)
        val errors = mutableListOf<String>()
        cfg.fields.forEach { (name, spec) ->
            val resolved = resolveJsonSchemaPath(root, spec.path.split('.').filter { it.isNotEmpty() })
            if (resolved == null) errors += "field '$name' path '${spec.path}' not found in JSON Schema"
        }
        return if (errors.isEmpty()) ProbeResult.Ok else ProbeResult.Invalid(errors)
    }

    private fun resolveJsonSchemaPath(
        node: JsonNode,
        parts: List<String>,
    ): JsonNode? {
        var cur: JsonNode? = node
        for (p in parts) {
            val props = cur?.get("properties") ?: return null
            cur = props.get(p) ?: return null
        }
        return cur
    }

    sealed interface ProbeResult {
        data object Ok : ProbeResult

        data class Invalid(
            val errors: List<String>,
        ) : ProbeResult

        data class Skipped(
            val reason: String,
        ) : ProbeResult
    }
}

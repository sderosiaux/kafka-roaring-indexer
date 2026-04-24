package io.conduktor.kri.ingest

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.conduktor.kri.config.FieldSpec
import io.conduktor.kri.config.IndexerConfig
import org.apache.avro.generic.GenericRecord

/**
 * Turns a deserialized payload (Avro GenericRecord, JSON node, or Map) into a Map<fieldName, raw>
 * where raw is whatever native type the spec type maps to.
 */
class RecordExtractor(
    private val cfg: IndexerConfig,
) {
    private val pathsByField: Map<String, List<String>> =
        cfg.fields.mapValues { (_, f) ->
            f.path.split('.').filter { it.isNotEmpty() }
        }

    fun extract(payload: Any?): Map<String, Any?> {
        if (payload == null) return emptyMap()
        val out = HashMap<String, Any?>(cfg.fields.size)
        cfg.fields.forEach { (name, spec) ->
            val raw = readPath(payload, pathsByField.getValue(name))
            out[name] = coerce(raw, spec)
        }
        return out
    }

    private fun coerce(
        raw: Any?,
        spec: FieldSpec,
    ): Any? {
        if (raw == null) return null
        return when (spec.type) {
            FieldSpec.FieldType.STRING -> raw.toString()
            FieldSpec.FieldType.BOOL ->
                when (raw) {
                    is Boolean -> raw
                    is Number -> raw.toInt() != 0
                    is String -> raw.equals("true", ignoreCase = true)
                    else -> null
                }
            FieldSpec.FieldType.FLOAT, FieldSpec.FieldType.DOUBLE ->
                when (raw) {
                    is Number -> raw.toDouble()
                    is String -> raw.toDoubleOrNull()
                    else -> null
                }
            FieldSpec.FieldType.BYTES -> raw
            FieldSpec.FieldType.UINT8, FieldSpec.FieldType.UINT16,
            FieldSpec.FieldType.UINT32, FieldSpec.FieldType.UINT64,
            FieldSpec.FieldType.INT32, FieldSpec.FieldType.INT64,
            ->
                when (raw) {
                    is Number -> raw.toLong()
                    is Boolean -> if (raw) 1L else 0L
                    is String -> raw.toLongOrNull()
                    else -> null
                }
        }
    }

    private fun readPath(
        root: Any?,
        parts: List<String>,
    ): Any? {
        var cur: Any? = root
        for (p in parts) {
            cur =
                when (cur) {
                    null -> return null
                    is Map<*, *> -> cur[p]
                    is JsonNode -> cur.get(p)?.let { jsonNodeToValue(it) }
                    is GenericRecord -> runCatching { cur.get(p) }.getOrNull()
                    else -> {
                        // Reflection-like fallback via getters.
                        try {
                            cur.javaClass.methods
                                .firstOrNull {
                                    it.name == "get${p.replaceFirstChar { c -> c.uppercase() }}" &&
                                        it.parameterCount == 0
                                }?.invoke(cur)
                        } catch (_: Exception) {
                            null
                        }
                    }
                }
        }
        return jsonLeafNormalize(cur)
    }

    private fun jsonLeafNormalize(v: Any?): Any? =
        when (v) {
            is JsonNode -> jsonNodeToValue(v)
            is CharSequence -> v.toString()
            else -> v
        }

    private fun jsonNodeToValue(n: JsonNode): Any? =
        when {
            n.isNull -> null
            n.isTextual -> n.asText()
            n.isBoolean -> n.asBoolean()
            n.isIntegralNumber -> n.asLong()
            n.isFloatingPointNumber -> n.asDouble()
            n.isObject || n.isArray -> n
            else -> n.asText()
        }

    companion object {
        internal val mapper: ObjectMapper = ObjectMapper().registerKotlinModule()
    }
}

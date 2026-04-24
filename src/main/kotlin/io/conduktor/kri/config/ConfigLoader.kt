package io.conduktor.kri.config

import com.charleskorn.kaml.Yaml
import com.charleskorn.kaml.YamlConfiguration
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.JsonSchema
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion
import java.nio.file.Files
import java.nio.file.Path

class ConfigValidationException(
    val errors: List<String>,
) : RuntimeException("Config validation failed:\n  - ${errors.joinToString("\n  - ")}")

object ConfigLoader {
    private val yaml = Yaml(configuration = YamlConfiguration(strictMode = false))
    private val jackson = ObjectMapper(YAMLFactory())
    private val schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)

    fun loadAndValidate(path: Path): IndexerConfig {
        val text = Files.readString(path)
        val schema = loadSchema()
        val jsonNode = jackson.readTree(text)
        val errors = schema.validate(jsonNode)
        if (errors.isNotEmpty()) {
            throw ConfigValidationException(errors.map { "${it.instanceLocation}: ${it.message}" })
        }
        val cfg = yaml.decodeFromString(IndexerConfig.serializer(), text)
        semanticValidate(cfg)
        return cfg
    }

    fun parseOnly(text: String): IndexerConfig = yaml.decodeFromString(IndexerConfig.serializer(), text)

    private fun loadSchema(): JsonSchema {
        val stream =
            ConfigLoader::class.java.classLoader.getResourceAsStream("indexer.schema.json")
                ?: error("indexer.schema.json missing from classpath")
        return stream.use { schemaFactory.getSchema(it) }
    }

    private fun semanticValidate(cfg: IndexerConfig) {
        val errors = mutableListOf<String>()

        if (cfg.member.field !in cfg.fields) {
            errors += "member.field '${cfg.member.field}' is not declared in fields"
        }
        val timeFieldName = cfg.timeField
        if (timeFieldName != null && timeFieldName !in cfg.fields) {
            errors += "timeField '$timeFieldName' is not declared in fields"
        }

        cfg.dimensions.forEach { d ->
            val f =
                cfg.fields[d.field]
                    ?: run {
                        errors += "dimension '${d.name}' references unknown field '${d.field}'"
                        return@forEach
                    }
            when (d.encoding) {
                Dimension.Encoding.DICT ->
                    if (!f.type.isStringLike() && !f.type.isInteger()) {
                        errors += "dimension '${d.name}' (dict) requires string/bytes/int field, got ${f.type}"
                    }
                Dimension.Encoding.RAW_UINT32 ->
                    if (!f.type.isInteger()) {
                        errors += "dimension '${d.name}' (raw_uint32) requires integer field, got ${f.type}"
                    }
                Dimension.Encoding.HASH32, Dimension.Encoding.HASH64 -> {
                    if (d.hashAlgo == null && d.encoding != Dimension.Encoding.DICT) {
                        // default handled at runtime; no error
                    }
                }
            }
        }

        cfg.metrics.forEach { m ->
            when (m.type) {
                Metric.Type.ULL, Metric.Type.HLL, Metric.Type.CPC -> {
                    if (m.field == null) errors += "metric '${m.name}' (${m.type}) requires a field"
                    if (m.precision == null) {
                        errors += "metric '${m.name}' (${m.type}) requires a precision"
                    } else if (m.precision !in 4..20) {
                        errors += "metric '${m.name}' precision out of [4..20]"
                    }
                }
                Metric.Type.SUM -> {
                    if (m.field == null) errors += "metric '${m.name}' (sum) requires a field"
                    val t = cfg.fields[m.field]?.type
                    if (t != null && !t.isInteger() && t !in setOf(FieldSpec.FieldType.FLOAT, FieldSpec.FieldType.DOUBLE)) {
                        errors += "metric '${m.name}' (sum) needs numeric field, got $t"
                    }
                }
                Metric.Type.COUNT -> {}
            }
            m.sliceBy?.forEach { slice ->
                if (cfg.dimensions.none { it.name == slice }) {
                    errors += "metric '${m.name}' sliceBy references unknown dimension '$slice'"
                }
            }
        }

        runCatching { parseDuration(cfg.segmentation.bucket) }
            .onFailure { errors += "segmentation.bucket: ${it.message}" }
        cfg.segmentation.retention?.let { r ->
            runCatching { parseDuration(r) }.onFailure { errors += "segmentation.retention: ${it.message}" }
        }

        if (errors.isNotEmpty()) throw ConfigValidationException(errors)
    }
}

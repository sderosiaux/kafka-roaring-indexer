package io.conduktor.kri.config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class IndexerConfig(
    val apiVersion: String,
    val kind: String,
    val metadata: Metadata,
    val source: Source,
    val segmentation: Segmentation,
    val fields: Map<String, FieldSpec>,
    val timeField: String? = null,
    val member: Member,
    val dimensions: List<Dimension>,
    val metrics: List<Metric> = emptyList(),
    val storage: Storage,
    val query: Query = Query(),
    val observability: Observability = Observability(),
    val experimentalModels: ExperimentalModels = ExperimentalModels(),
)

/**
 * Opt-in alternative index models, built in parallel to the canonical Roaring + ULL path.
 *
 * Existing Roaring/ULL state is never replaced — these models add structures that the query
 * layer can target via the `models=` HTTP parameter for side-by-side comparison.
 */
@Serializable
data class ExperimentalModels(
    /** Numeric dim names that get a Bit-Sliced Index in addition to the bucketed Roaring. */
    val bsiDims: List<String> = emptyList(),
    /** When true, build a per-segment theta sample of memberIds for distinct-under-filter. */
    val thetaSample: Boolean = false,
    /** Fraction of memberIds kept by the theta sample. 1.0 = all. Default 1/16 ≈ 6.25%. */
    val thetaSampleRate: Double = 0.0625,
    /** When true, build a joint-profile dictionary at freeze. */
    val jointProfile: Boolean = false,
    /**
     * When true, compute a member-axis permutation at freeze and build reordered Roaring.
     * Default ON: ~25-40% smaller dim bitmaps + faster AND/OR on tightly-clustered users, at
     * the cost of one extra freeze pass. Evaluator auto-uses reordered when present.
     */
    val reorderMembers: Boolean = true,
)

@Serializable
data class Metadata(
    val name: String,
    val description: String? = null,
)

@Serializable
data class Source(
    val topic: String,
    val bootstrap: String,
    val consumerGroup: String,
    val startOffset: String = "latest",
    val schemaRegistry: SchemaRegistry? = null,
    val parallelism: Int = 1,
)

@Serializable
data class SchemaRegistry(
    val url: String,
    val format: Format,
) {
    @Serializable
    enum class Format {
        @SerialName("avro")
        AVRO,

        @SerialName("protobuf")
        PROTOBUF,

        @SerialName("json-schema")
        JSON_SCHEMA,

        @SerialName("json")
        JSON,
    }
}

@Serializable
data class Segmentation(
    val strategy: Strategy = Strategy.TIME,
    val bucket: String,
    val alignment: Alignment = Alignment.UTC,
    val retention: String? = null,
    val maxOpenSegments: Int = 2,
) {
    @Serializable
    enum class Strategy {
        @SerialName("time")
        TIME,

        @SerialName("record_count")
        RECORD_COUNT,

        @SerialName("hybrid")
        HYBRID,
    }

    @Serializable
    enum class Alignment {
        @SerialName("utc")
        UTC,

        @SerialName("none")
        NONE,
    }
}

@Serializable
data class FieldSpec(
    val path: String,
    val type: FieldType,
    val unit: TimeUnitSpec? = null,
) {
    @Serializable
    enum class FieldType {
        @SerialName("string")
        STRING,

        @SerialName("bytes")
        BYTES,

        @SerialName("uint8")
        UINT8,

        @SerialName("uint16")
        UINT16,

        @SerialName("uint32")
        UINT32,

        @SerialName("uint64")
        UINT64,

        @SerialName("int32")
        INT32,

        @SerialName("int64")
        INT64,

        @SerialName("float")
        FLOAT,

        @SerialName("double")
        DOUBLE,

        @SerialName("bool")
        BOOL,

        ;

        fun isInteger() = this in setOf(UINT8, UINT16, UINT32, UINT64, INT32, INT64)

        fun isStringLike() = this in setOf(STRING, BYTES)
    }

    @Serializable
    enum class TimeUnitSpec {
        @SerialName("ms")
        MS,

        @SerialName("us")
        US,

        @SerialName("ns")
        NS,

        @SerialName("s")
        S,
    }
}

@Serializable
data class Member(
    val field: String,
    val encoding: Encoding,
    val dict: DictConfig? = null,
) {
    @Serializable
    enum class Encoding {
        @SerialName("raw_uint32")
        RAW_UINT32,

        @SerialName("dict")
        DICT,

        @SerialName("hash64")
        HASH64,
    }
}

@Serializable
data class DictConfig(
    val persist: Boolean = true,
    val snapshotEvery: String? = null,
    val maxSize: Long? = null,
    val onOverflow: OverflowMode = OverflowMode.REJECT,
)

@Serializable
enum class OverflowMode {
    @SerialName("reject")
    REJECT,

    @SerialName("overflow")
    OVERFLOW,

    @SerialName("halt")
    HALT,
}

@Serializable
data class Dimension(
    val name: String,
    val field: String,
    val encoding: Encoding,
    val hashAlgo: HashAlgo? = null,
    val maxCardinality: Int? = null,
    val onOverflow: OverflowMode = OverflowMode.REJECT,
    val bucket: BucketSpec? = null,
) {
    @Serializable
    enum class Encoding {
        @SerialName("dict")
        DICT,

        @SerialName("raw_uint32")
        RAW_UINT32,

        @SerialName("hash32")
        HASH32,

        @SerialName("hash64")
        HASH64,
    }

    @Serializable
    enum class HashAlgo {
        @SerialName("murmur3")
        MURMUR3,

        @SerialName("xxh3")
        XXH3,
    }
}

@Serializable
data class BucketSpec(
    val type: Type,
    val step: Double? = null,
    val base: Double? = null,
    val min: Double? = null,
    val max: Double? = null,
    val boundaries: List<Double>? = null,
) {
    @Serializable
    enum class Type {
        @SerialName("linear")
        LINEAR,

        @SerialName("exponential")
        EXPONENTIAL,

        @SerialName("explicit")
        EXPLICIT,
    }
}

@Serializable
data class Metric(
    val name: String,
    val type: Type,
    val field: String? = null,
    val precision: Int? = null,
    val hashSeed: Long? = null,
    val sliceBy: List<String>? = null,
) {
    @Serializable
    enum class Type {
        @SerialName("ull")
        ULL,

        @SerialName("hll")
        HLL,

        @SerialName("cpc")
        CPC,

        @SerialName("count")
        COUNT,

        @SerialName("sum")
        SUM,
    }
}

@Serializable
data class Storage(
    val path: String,
    val format: String = "portable",
    val compress: Compress = Compress.ZSTD,
    val mmap: Boolean = true,
    val flushOnRoll: Boolean = true,
) {
    @Serializable
    enum class Compress {
        @SerialName("none")
        NONE,

        @SerialName("zstd")
        ZSTD,
    }
}

@Serializable
data class Query(
    val http: HttpQuery = HttpQuery(),
    val maxSegmentsPerQuery: Int = 720,
    val cachePlans: Boolean = true,
    /** Max threads for per-segment fan-out. null = availableProcessors(). 1 = force sequential. */
    val parallelism: Int? = null,
    /** Segments >= this threshold switch to parallel eval; below, sequential (overhead avoidance). */
    val parallelThreshold: Int = 8,
    /**
     * Other KRI instances to fan-out queries to (Option C multi-consumer).
     * Self is always queried in-memory; list only the other peers.
     * e.g. ["http://kri-1.kri:8080", "http://kri-2.kri:8080"]
     */
    val peers: List<String> = emptyList(),
)

@Serializable
data class HttpQuery(
    val bind: String = "0.0.0.0:8080",
    val auth: Auth = Auth.NONE,
) {
    @Serializable
    enum class Auth {
        @SerialName("none")
        NONE,

        @SerialName("bearer")
        BEARER,

        @SerialName("mtls")
        MTLS,
    }
}

@Serializable
data class Observability(
    val metrics: MetricsObs = MetricsObs(),
    val log: LogObs = LogObs(),
    val sample: SampleObs = SampleObs(),
)

@Serializable
data class MetricsObs(
    val prometheus: PromObs? = null,
)

@Serializable
data class PromObs(
    val bind: String = "0.0.0.0:9090",
)

@Serializable
data class LogObs(
    val level: String = "info",
)

@Serializable
data class SampleObs(
    val rejectedRecords: Double = 0.0,
)

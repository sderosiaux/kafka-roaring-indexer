package io.conduktor.kri.index

import com.dynatrace.hash4j.hashing.Hashing
import io.conduktor.kri.config.Dimension
import io.conduktor.kri.config.Member
import io.conduktor.kri.config.OverflowMode

/** Result of encoding a value for a dim/member. `Skip` means the record should be dropped for this dim. */
sealed interface EncodeResult {
    data class Ok(
        val id: Int,
    ) : EncodeResult

    data object Overflow : EncodeResult

    data object Skip : EncodeResult
}

/** Dimension-level encoder: value (Any) → uint32 id. Stateful (owns dicts). */
interface DimEncoder {
    val dimName: String

    fun encode(raw: Any?): EncodeResult

    fun dict(): Dict? = null

    fun cardinality(): Int
}

class DictEncoder(
    override val dimName: String,
    private val maxCardinality: Int?,
    private val overflow: OverflowMode,
    private val dict: Dict = Dict(),
) : DimEncoder {
    override fun encode(raw: Any?): EncodeResult {
        if (raw == null) return EncodeResult.Skip
        val s = raw.toString()
        val id = dict.getOrIntern(s, maxCardinality)
        return when {
            id >= 0 -> EncodeResult.Ok(id)
            overflow == OverflowMode.OVERFLOW -> EncodeResult.Overflow
            overflow == OverflowMode.REJECT -> EncodeResult.Overflow
            else -> EncodeResult.Overflow
        }
    }

    override fun dict(): Dict = dict

    override fun cardinality(): Int = dict.size()
}

class RawUInt32Encoder(
    override val dimName: String,
) : DimEncoder {
    private var observed = HashSet<Int>()

    override fun encode(raw: Any?): EncodeResult {
        if (raw == null) return EncodeResult.Skip
        val v =
            when (raw) {
                is Number -> raw.toLong()
                is String -> raw.toLongOrNull() ?: return EncodeResult.Skip
                is Boolean -> if (raw) 1L else 0L
                else -> return EncodeResult.Skip
            }
        if (v < 0 || v > 0xFFFF_FFFFL) return EncodeResult.Skip
        val id = v.toInt()
        observed.add(id)
        return EncodeResult.Ok(id)
    }

    override fun cardinality(): Int = observed.size
}

class Hash32Encoder(
    override val dimName: String,
    algo: Dimension.HashAlgo,
) : DimEncoder {
    private val observed = HashSet<Int>()
    private val h = pickHasher64(algo)

    override fun encode(raw: Any?): EncodeResult {
        if (raw == null) return EncodeResult.Skip
        val h64 = h.hashBytesToLong(raw.toString().toByteArray())
        val id = ((h64 xor (h64 ushr 32)).toInt())
        observed.add(id)
        return EncodeResult.Ok(id)
    }

    override fun cardinality(): Int = observed.size
}

class Hash64Encoder(
    override val dimName: String,
    algo: Dimension.HashAlgo,
) : DimEncoder {
    private val observed = HashSet<Int>()
    private val h = pickHasher64(algo)

    override fun encode(raw: Any?): EncodeResult {
        if (raw == null) return EncodeResult.Skip
        val h64 = h.hashBytesToLong(raw.toString().toByteArray())
        val id = ((h64 xor (h64 ushr 32)).toInt())
        observed.add(id)
        return EncodeResult.Ok(id)
    }

    override fun cardinality(): Int = observed.size
}

private fun pickHasher64(algo: Dimension.HashAlgo): com.dynatrace.hash4j.hashing.Hasher64 =
    when (algo) {
        Dimension.HashAlgo.MURMUR3 -> Hashing.murmur3_128()
        Dimension.HashAlgo.XXH3 -> Hashing.xxh3_64()
    }

/** Member encoder separate type — member id semantic may differ (distinct user vs offset). */
class MemberEncoder(
    private val member: Member,
    private val dict: Dict? = null,
) {
    fun encode(
        raw: Any?,
        overflow: OverflowMode = OverflowMode.REJECT,
        maxSize: Long? = null,
    ): Int? {
        if (raw == null) return null
        return when (member.encoding) {
            Member.Encoding.RAW_UINT32 ->
                when (raw) {
                    is Number -> {
                        val v = raw.toLong()
                        if (v < 0 || v > 0xFFFF_FFFFL) null else v.toInt()
                    }
                    is String -> raw.toLongOrNull()?.let { if (it < 0 || it > 0xFFFF_FFFFL) null else it.toInt() }
                    else -> null
                }
            Member.Encoding.HASH64 -> {
                val h = Hashing.komihash5_0().hashBytesToLong(raw.toString().toByteArray())
                ((h xor (h ushr 32)).toInt())
            }
            Member.Encoding.DICT -> {
                val d = dict ?: error("member.encoding=dict requires a dict instance")
                val limit = maxSize?.let { if (it > Int.MAX_VALUE) Int.MAX_VALUE else it.toInt() }
                val id = d.getOrIntern(raw.toString(), limit)
                if (id < 0) null else id
            }
        }
    }

    fun dict(): Dict? = dict
}

object EncoderFactory {
    fun forDimension(d: Dimension): DimEncoder =
        when (d.encoding) {
            Dimension.Encoding.DICT -> DictEncoder(d.name, d.maxCardinality, d.onOverflow)
            Dimension.Encoding.RAW_UINT32 -> RawUInt32Encoder(d.name)
            Dimension.Encoding.HASH32 -> Hash32Encoder(d.name, d.hashAlgo ?: Dimension.HashAlgo.MURMUR3)
            Dimension.Encoding.HASH64 -> Hash64Encoder(d.name, d.hashAlgo ?: Dimension.HashAlgo.XXH3)
        }
}

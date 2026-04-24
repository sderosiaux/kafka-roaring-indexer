package io.conduktor.kri.index

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** Append-only value → uint32 id mapping. Never reassigns ids. */
class Dict(
    initial: Map<String, Int> = emptyMap(),
) {
    private val map = ConcurrentHashMap<String, Int>(initial)
    private val nextId = AtomicInteger(initial.values.maxOrNull()?.plus(1) ?: 0)

    fun size(): Int = map.size

    fun lookup(value: String): Int? = map[value]

    /** Returns existing id or -1 if full (bound by [limit]). */
    fun getOrIntern(
        value: String,
        limit: Int? = null,
    ): Int {
        map[value]?.let { return it }
        return map
            .computeIfAbsent(value) {
                if (limit != null && map.size >= limit) return@computeIfAbsent -1
                nextId.getAndIncrement()
            }.also { if (it == -1) map.remove(value) }
    }

    fun entries(): List<Pair<String, Int>> = map.entries.map { it.key to it.value }.sortedBy { it.second }

    fun toMap(): Map<String, Int> = map.toMap()

    companion object {
        fun fromEntries(entries: List<Pair<String, Int>>): Dict = Dict(entries.toMap())
    }
}

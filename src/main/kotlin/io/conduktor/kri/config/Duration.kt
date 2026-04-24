package io.conduktor.kri.config

import java.time.Duration

private val DURATION_RE = Regex("^([0-9]+)(ms|s|m|h|d)$")

fun parseDuration(s: String): Duration {
    val m = DURATION_RE.matchEntire(s) ?: error("Invalid duration: $s (expected like 1h, 30d, 500ms)")
    val n = m.groupValues[1].toLong()
    return when (m.groupValues[2]) {
        "ms" -> Duration.ofMillis(n)
        "s" -> Duration.ofSeconds(n)
        "m" -> Duration.ofMinutes(n)
        "h" -> Duration.ofHours(n)
        "d" -> Duration.ofDays(n)
        else -> error("unreachable")
    }
}

package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.decay.DecayingSumResult
import com.eignex.kumulant.stat.decay.DecayingSumStat
import kotlin.math.exp
import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.nanoseconds

class DecayingSumRotationTest {

    @Test
    fun `a concurrent stream across many epoch rotations stays finite`() {
        // A 1us half-life forces frequent landmark rotation while writers race.
        val stat = DecayingSumStat(halfLife = 1.microseconds, concurrency = Concurrency.Relaxed)
        val threads = 4
        val perThread = 20_000
        runConcurrently(threads, perThread) { t, i ->
            stat.update(1.0, timestampNanos = i.toLong() * 1000L + t)
        }
        val sum = stat.read((perThread.toLong() - 1) * 1000L + threads).sum
        assertTrue(sum.isFinite() && sum > 0.0, "sum=$sum")
    }

    @Test
    fun `concurrent updates and merges retain their contributions across rotations`() {
        val size = 64
        val steps = 12
        val finalTimestamp = (steps - 1) * 51L
        // Normalize every contribution to one at the final read so decay cannot hide lost updates.
        val alpha = ln(2.0)
        for (level in listOf(Concurrency.Relaxed, Concurrency.Strict, Concurrency.HighWrite)) {
            for (merge in listOf(false, true)) {
                val stats = List(size) {
                    DecayingSumStat(1.nanoseconds, level).apply {
                        update(exp(alpha * finalTimestamp), timestampNanos = 0L)
                    }
                }

                runConcurrently(4, size * steps) { thread, i ->
                    val timestamp = if (thread == 0) 0L else (i / size) * 51L
                    val value = exp(alpha * (finalTimestamp - timestamp))
                    val stat = stats[i % size]
                    if (merge) stat.merge(DecayingSumResult(value, timestamp)) else stat.update(value, timestamp)
                }

                stats.forEachIndexed { index, stat ->
                    assertEquals(
                        1.0 + 4 * steps,
                        stat.read(finalTimestamp).sum,
                        1e-8,
                        "$level merge=$merge stat=$index",
                    )
                }
            }
        }
    }
}

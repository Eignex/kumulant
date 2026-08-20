package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.decay.DecayingSumStat
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.microseconds

class DecayingSumRotationTest {

    @Test
    fun `a concurrent stream across many epoch rotations stays finite`() {
        // A 1us half-life puts the rotation threshold at 50us, so a stream stepping 1us per update
        // crosses it constantly. Writers race the rotation from both sides: one adding against a
        // landmark another is replacing, and one arriving so far behind the landmark that its
        // exponent underflows while the read's matching exponent overflows.
        val stat = DecayingSumStat(halfLife = 1.microseconds, concurrency = Concurrency.Relaxed)
        val threads = 4
        val perThread = 20_000
        runConcurrently(threads, perThread) { t, i ->
            stat.update(1.0, timestampNanos = i.toLong() * 1000L + t)
        }
        val sum = stat.read((perThread.toLong() - 1) * 1000L + threads).sum
        assertTrue(sum.isFinite() && sum > 0.0, "sum=$sum")
    }
}

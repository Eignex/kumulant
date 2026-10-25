package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.operation.diff
import com.eignex.kumulant.stat.summary.MaxStat
import kotlin.test.Test
import kotlin.test.assertTrue

class ShiftRingRaceTest {

    @Test
    fun `diff never forwards a difference against a value the stream never emitted`() {
        // Constant observations make every valid difference zero; an uninitialised slot would emit 1e6.
        val stat = MaxStat(Concurrency.Strict).diff(1)
        runConcurrently(threads = 8, iterationsPerThread = 2000) { _, _ ->
            stat.update(1_000_000.0)
        }
        val largest = stat.read(0L).max
        assertTrue(largest <= 1.0, "forwarded a fabricated difference: $largest")
    }
}

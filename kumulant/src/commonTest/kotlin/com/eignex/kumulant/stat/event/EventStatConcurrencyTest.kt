package com.eignex.kumulant.stat.event

import com.eignex.kumulant.WEIGHTED_WEIGHTS
import com.eignex.kumulant.assertModesAgree
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.weightedReads
import kotlin.test.Test

/**
 * Cross-mode smoke tests for the event family: each stat should produce identical math
 * under sequential single-threaded updates regardless of the [Concurrency] mode chosen.
 */
class EventStatConcurrencyTest {

    @Test
    fun `ExcursionStat sequential math equal across modes`() {
        val reads = weightedReads { ExcursionStat(concurrency = it) }
        assertModesAgree("ExcursionStat", reads)
    }

    @Test
    fun `RunLengthStat sequential math equal across modes`() {
        val flagValues = doubleArrayOf(1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0)
        val reads = Concurrency.entries.associateWith { mode ->
            val s = RunLengthStat(concurrency = mode)
            for (i in flagValues.indices) s.update(flagValues[i], 0L, WEIGHTED_WEIGHTS[i])
            s.read(0L)
        }
        assertModesAgree("RunLengthStat", reads)
    }

    @Test
    fun `CrossingStat sequential math equal across modes`() {
        val reads = weightedReads { CrossingStat(level = 1.0, concurrency = it) }
        assertModesAgree("CrossingStat", reads)
    }
}

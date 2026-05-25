package com.eignex.kumulant.stat.event

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Cross-mode smoke tests for the event family: each stat should produce identical math
 * under sequential single-threaded updates regardless of the [Concurrency] mode chosen.
 */
class EventStatConcurrencyTest {

    private val values = doubleArrayOf(1.0, -2.0, 3.5, 0.0, 4.2, -1.1, 7.0, 2.5)
    private val weights = doubleArrayOf(1.0, 2.0, 1.0, 3.0, 1.0, 1.0, 2.5, 0.5)

    private fun <R : Result> sequentialReads(factory: (Concurrency) -> SeriesStat<R>): Map<Concurrency, R> =
        Concurrency.entries.associateWith { mode ->
            val s = factory(mode)
            for (i in values.indices) s.update(values[i], 0L, weights[i])
            s.read(0L)
        }

    @Test
    fun `ExcursionStat sequential math equal across modes`() {
        val reads = sequentialReads { ExcursionStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "ExcursionStat mode=$mode")
    }

    @Test
    fun `RunLengthStat sequential math equal across modes`() {
        val flagValues = doubleArrayOf(1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0)
        val reads = Concurrency.entries.associateWith { mode ->
            val s = RunLengthStat(concurrency = mode)
            for (i in flagValues.indices) s.update(flagValues[i], 0L, weights[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "RunLengthStat mode=$mode")
    }

    @Test
    fun `CrossingStat sequential math equal across modes`() {
        val reads = sequentialReads { CrossingStat(level = 1.0, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "CrossingStat mode=$mode")
    }
}

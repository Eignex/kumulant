package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals

class ResettableCountersTest {
    @Test
    fun `sum reset clears non-finite state in every concurrency mode`() {
        for (concurrency in Concurrency.entries) {
            for (value in listOf(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
                val stat = SumStat(concurrency)
                stat.update(value, 0L)
                stat.reset()
                assertEquals(0.0, stat.read(0L).sum, "mode=$concurrency value=$value")
                stat.update(7.0, 0L)
                assertEquals(7.0, stat.read(0L).sum, "mode=$concurrency value=$value")
            }
        }
    }

    @Test
    fun `reset counters retain subsequent concurrent additions`() {
        for (concurrency in Concurrency.entries.filter { it != Concurrency.None }) {
            val mode = concurrency.resettableMode()
            val doubles = mode.newDouble(3.0)
            val longs = mode.newLong(5L)
            repeat(2) {
                runConcurrently(4, 1_000) { _, _ ->
                    doubles.add(1.0)
                    longs.add(1L)
                }
                assertEquals(4_003.0, doubles.load(), "mode=$concurrency")
                assertEquals(4_005L, longs.load(), "mode=$concurrency")
                doubles.reset()
                longs.reset()
                assertEquals(3.0, doubles.load(), "mode=$concurrency")
                assertEquals(5L, longs.load(), "mode=$concurrency")
            }
        }
    }
}

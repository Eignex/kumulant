package com.eignex.kumulant.stat.cardinality

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals

class CardinalityStatConcurrencyTest {

    private val keys = longArrayOf(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 1L, 2L)

    @Test
    fun `LinearCountingStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = LinearCountingStat(bits = 256, concurrency = mode)
            for (k in keys) s.update(k)
            s.read(0L)
        }
        val ref = reads[Concurrency.None]!!
        for ((mode, r) in reads) assertEquals(ref, r, "LinearCountingStat mode=$mode")
    }

    @Test
    fun `HyperLogLogStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = HyperLogLogStat(precision = 8, concurrency = mode)
            for (k in keys) s.update(k)
            s.read(0L)
        }
        val ref = reads[Concurrency.None]!!
        for ((mode, r) in reads) assertEquals(ref.estimate, r.estimate, "HyperLogLogStat mode=$mode")
    }
}

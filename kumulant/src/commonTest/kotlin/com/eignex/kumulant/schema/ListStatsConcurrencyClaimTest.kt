package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.runtime.ListStats
import com.eignex.kumulant.schema.runtime.StatGroup
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals

// A container is only as safe as its least-protected member, and this property is what a caller
// introspects to decide whether reads need external synchronisation. `AbstractStatGroup` carries the
// same rule, and the two must agree over identical entries or the property is unreliable rather than
// merely pessimistic.
class ListStatsConcurrencyClaimTest {

    @Test
    fun `a list of strict stats does not claim to be unsynchronised`() {
        val stats = ListStats("a" to SumStat(Concurrency.Strict), "b" to SumStat(Concurrency.Strict))

        assertEquals(Concurrency.Strict, stats.concurrency, "a list of Strict entries reported otherwise")
    }

    @Test
    fun `one weak entry drags the whole list down to it`() {
        // The direction that matters for safety: a caller must not be told the list is Strict when one
        // entry inside it is not.
        val stats = ListStats("strict" to SumStat(Concurrency.Strict), "none" to SumStat(Concurrency.None))

        assertEquals(Concurrency.None, stats.concurrency, "one unsynchronised entry was not reported")
    }

    @Test
    fun `a requested mode does not weaken existing children`() {
        val stats = ListStats(
            listOf("a" to SumStat(Concurrency.Strict)),
            concurrency = Concurrency.Relaxed,
        )

        assertEquals(Concurrency.Strict, stats.concurrency)
    }

    @Test
    fun `an empty list falls back to None`() {
        val stats = ListStats<SumResult>(emptyList())

        assertEquals(Concurrency.None, stats.concurrency)
    }

    @Test
    fun `requested modes cannot strengthen supplied children`() {
        for (requested in Concurrency.entries) {
            val list = ListStats(listOf("sum" to SumStat()), concurrency = requested)
            val group = StatGroup(
                StatKey<SumResult>("sum") to SumStat(),
                concurrency = requested,
            )

            assertEquals(Concurrency.None, list.concurrency, "$requested list")
            assertEquals(Concurrency.None, group.concurrency, "$requested group")
        }
    }

    @Test
    fun `creating without a mode preserves child guarantees`() {
        val list = ListStats(listOf("sum" to SumStat(Concurrency.Strict)), concurrency = Concurrency.None)

        val copy = list.create(null)

        assertEquals(Concurrency.Strict, copy.concurrency)
    }

    @Test
    fun `strict and high write children combine independently of order`() {
        for ((first, second) in listOf(
            Concurrency.Strict to Concurrency.HighWrite,
            Concurrency.HighWrite to Concurrency.Strict,
        )) {
            val list = ListStats("a" to SumStat(first), "b" to SumStat(second))

            assertEquals(Concurrency.Strict, list.concurrency)
        }
    }
}

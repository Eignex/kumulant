package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.runtime.ListStats
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
    fun `an explicit override still wins`() {
        val stats = ListStats(
            listOf("a" to SumStat(Concurrency.Strict)),
            concurrency = Concurrency.Relaxed,
        )

        assertEquals(Concurrency.Relaxed, stats.concurrency, "the override was ignored")
    }

    @Test
    fun `an empty list falls back to None`() {
        val stats = ListStats<SumResult>(emptyList())

        assertEquals(Concurrency.None, stats.concurrency)
    }
}

package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SpaceSavingAdmissionPolicyTest {

    private fun feed(stat: SpaceSavingStat, distinctKeys: Int, perKey: Int) {
        for (k in 0 until distinctKeys) {
            repeat(perKey) { stat.update(k.toLong()) }
        }
    }

    @Test
    fun `each concurrency level reports the policy it actually runs`() {
        val expected = mapOf(
            Concurrency.None to AdmissionPolicy.Classic,
            Concurrency.Strict to AdmissionPolicy.Classic,
            Concurrency.HighWrite to AdmissionPolicy.Classic,
            Concurrency.Relaxed to AdmissionPolicy.MisraGries,
        )
        val violations = mutableListOf<String>()
        for (level in Concurrency.entries) {
            val want = expected[level]
            if (want == null) {
                violations += "$level has no expectation here, so this sweep does not cover it"
                continue
            }
            val stat = SpaceSavingStat(capacity = 4, concurrency = level)
            feed(stat, distinctKeys = 3, perKey = 2)
            val got = stat.read().policy
            if (got != want) violations += "$level reported $got, runs $want"
        }
        assertEquals(emptyList(), violations.toList(), "a level misreports its admission algorithm")
    }

    @Test
    fun `classic never underestimates so it reports no deficit`() {
        val stat = SpaceSavingStat(capacity = 4, concurrency = Concurrency.None)
        feed(stat, distinctKeys = 20, perKey = 3)

        val r = stat.read()
        assertEquals(AdmissionPolicy.Classic, r.policy)
        assertEquals(0L, r.deficit, "classic inherits the evicted count, so no count can fall short")
        assertTrue(r.errors.any { it > 0L }, "evictions should have produced a per-key overestimate bound")
    }

    @Test
    fun `misra-gries reports the shortfall its decrements cause`() {
        val stat = SpaceSavingStat(capacity = 4, concurrency = Concurrency.Relaxed)
        feed(stat, distinctKeys = 40, perKey = 2)

        val r = stat.read()
        assertEquals(AdmissionPolicy.MisraGries, r.policy)
        assertTrue(r.deficit > 0L, "decrement rounds ran but the shortfall bound is still zero")
        assertTrue(r.errors.all { it == 0L }, "misra-gries counts cannot overestimate, so errors stays zero")
    }

    @Test
    fun `the reported bounds actually contain the true counts`() {
        for (level in listOf(Concurrency.None, Concurrency.Relaxed)) {
            val stat = SpaceSavingStat(capacity = 8, concurrency = level)
            val heavy = 7L
            val heavyCount = 500
            repeat(heavyCount) { stat.update(heavy) }
            for (k in 100 until 200) stat.update(k.toLong())
            repeat(heavyCount) { stat.update(heavy) }

            val r = stat.read()
            val idx = r.keys.indexOfFirst { it == heavy }
            assertTrue(idx >= 0, "$level: the heavy key was evicted, so this proves nothing")

            val truth = (heavyCount * 2).toLong()
            val low = r.counts[idx] - r.errors[idx]
            val high = r.counts[idx] + r.deficit
            assertTrue(
                truth in low..high,
                "$level: true=$truth outside [$low, $high] (count=${r.counts[idx]}, " +
                    "error=${r.errors[idx]}, deficit=${r.deficit})",
            )
        }
    }

    @Test
    fun `a mixed merge is absorbed rather than refused`() {
        val worker = SpaceSavingStat(capacity = 8, concurrency = Concurrency.Relaxed)
        feed(worker, distinctKeys = 40, perKey = 2)
        val snapshot = worker.read()
        assertEquals(AdmissionPolicy.MisraGries, snapshot.policy)
        assertTrue(snapshot.deficit > 0L, "the worker did not accumulate a shortfall, so this is vacuous")

        val coordinator = SpaceSavingStat(capacity = 8, concurrency = Concurrency.Strict)
        coordinator.update(1L)
        coordinator.merge(snapshot)

        val r = coordinator.read()
        assertEquals(
            AdmissionPolicy.MisraGries,
            r.policy,
            "a coordinator holding Misra-Gries counts must not claim the classic guarantee",
        )
        assertTrue(r.deficit >= snapshot.deficit, "the absorbed shortfall was dropped")
    }

    @Test
    fun `a same-policy merge keeps the stronger guarantee`() {
        val a = SpaceSavingStat(capacity = 8, concurrency = Concurrency.None)
        feed(a, distinctKeys = 20, perKey = 3)
        val b = SpaceSavingStat(capacity = 8, concurrency = Concurrency.Strict)
        b.merge(a.read())

        val r = b.read()
        assertEquals(AdmissionPolicy.Classic, r.policy, "two classic sketches merged should stay classic")
        assertEquals(0L, r.deficit)
    }

    @Test
    fun `reset clears the absorbed policy and shortfall`() {
        val stat = SpaceSavingStat(capacity = 8, concurrency = Concurrency.Strict)
        val worker = SpaceSavingStat(capacity = 8, concurrency = Concurrency.Relaxed)
        feed(worker, distinctKeys = 40, perKey = 2)
        stat.merge(worker.read())
        stat.reset()

        val r = stat.read()
        assertEquals(AdmissionPolicy.Classic, r.policy, "reset should restore the stat's own policy")
        assertEquals(0L, r.deficit, "reset should clear the absorbed shortfall")
    }
}

package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class FrugalMadStatTest {

    @Test
    fun `empty estimates are NaN`() {
        val result = FrugalMadStat().read()

        assertTrue(result.median.isNaN())
        assertTrue(result.mad.isNaN())
    }

    @Test
    fun `the first observation seeds the center with zero deviation`() {
        val stat = FrugalMadStat()

        stat.update(42.0)

        assertEquals(MadResult(42.0, 0.0), stat.read())
    }

    @Test
    fun `step size must be finite and positive`() {
        for (step in listOf(0.0, -1.0, Double.NaN, Double.POSITIVE_INFINITY)) {
            assertFailsWith<IllegalArgumentException> { FrugalMadStat(step) }
        }
    }

    @Test
    fun `an outlier moves each estimate by at most half a step`() {
        val stat = FrugalMadStat(stepSize = 2.0).apply { update(0.0) }

        stat.update(1_000_000.0)

        assertEquals(MadResult(1.0, 1.0), stat.read())
    }

    @Test
    fun `adjustments do not overshoot their targets`() {
        val stat = FrugalMadStat(stepSize = 10.0).apply { update(0.0) }

        stat.update(1.0)

        assertEquals(MadResult(1.0, 1.0), stat.read())
    }

    @Test
    fun `estimates adapt to a shifted constant stream`() {
        val stat = FrugalMadStat(stepSize = 2.0).apply { repeat(10) { update(0.0) } }

        repeat(1_000) { stat.update(100.0) }

        assertEquals(MadResult(100.0, 0.0), stat.read())
    }

    @Test
    fun `deviation tracks symmetric residuals`() {
        val stat = FrugalMadStat(stepSize = 0.02).apply { update(0.0) }

        repeat(1_000) {
            stat.update(-1.0)
            stat.update(1.0)
        }

        assertEquals(1.0, stat.read().mad, 0.02)
    }

    @Test
    fun `inert weights leave empty and populated estimates unchanged`() {
        for (weight in listOf(0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            for (primed in listOf(false, true)) {
                val stat = FrugalMadStat()
                if (primed) stat.update(7.0)
                val before = stat.read()

                stat.update(100.0, weight = weight)

                assertEquals(before, stat.read())
            }
        }
    }

    @Test
    fun `positive weights each trigger one adaptive step`() {
        val unit = FrugalMadStat().apply { update(0.0) }
        val weighted = FrugalMadStat().apply { update(0.0) }

        unit.update(100.0)
        weighted.update(100.0, weight = 50.0)

        assertEquals(unit.read(), weighted.read())
    }

    @Test
    fun `negative weights are rejected without changing state`() {
        val stat = FrugalMadStat().apply { update(7.0) }
        val before = stat.read()

        assertFailsWith<IllegalArgumentException> { stat.update(100.0, weight = -1.0) }

        assertEquals(before, stat.read())
    }

    @Test
    fun `independent estimates cannot be merged`() {
        val stat = FrugalMadStat().apply { update(7.0) }
        val before = stat.read()

        assertFailsWith<UnsupportedOperationException> { stat.validateMerge(MadResult(1.0, 2.0)) }
        assertFailsWith<UnsupportedOperationException> { stat.merge(MadResult(1.0, 2.0)) }

        assertEquals(before, stat.read())
    }

    @Test
    fun `reset seeds the next observation independently`() {
        val stat = FrugalMadStat().apply { update(100.0) }

        stat.reset()
        stat.update(-42.0)

        assertEquals(MadResult(-42.0, 0.0), stat.read())
    }

    @Test
    fun `create preserves step size and isolates state`() {
        val template = FrugalMadStat(stepSize = 2.0).apply { update(7.0) }

        val fresh = template.create(Concurrency.Strict)
        fresh.update(0.0)
        fresh.update(100.0)

        assertEquals(Concurrency.Strict, fresh.concurrency)
        assertEquals(1.0, fresh.read().median, DELTA)
        assertEquals(7.0, template.read().median, DELTA)
    }

    @Test
    fun `all concurrency modes agree for the same observation order`() {
        val results = Concurrency.entries.map { mode ->
            FrugalMadStat(concurrency = mode).apply {
                listOf(0.0, 1.0, -1.0, 100.0, 0.5).forEach { update(it) }
            }.read()
        }

        results.forEach { assertEquals(results.first(), it) }
    }
}

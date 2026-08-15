package com.eignex.kumulant.stat.forecast

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Rolling worker snapshots up into a fresh coordinator stat. None of the three smoothing stats has a
 * principled merge of two independent traces, but all three must at least adopt the first snapshot
 * verbatim rather than averaging it against their own empty state.
 */
class ForecastMergeTest {

    @Test
    fun `a recursive variance snapshot merges into a fresh stat unchanged`() {
        val source = RecursiveVarianceStat(omega = 0.1, alpha = 0.2, beta = 0.7)
        source.update(3.0, 1L)
        val expected = source.read().variance

        val fresh = source.create()
        fresh.merge(source.read())

        // This used to average against the fresh stat's 0.0 and halve the contribution.
        assertEquals(expected, fresh.read().variance, 1e-12)
    }

    @Test
    fun `rolling several recursive variance snapshots up does not shrink toward zero`() {
        val source = RecursiveVarianceStat(omega = 0.1, alpha = 0.2, beta = 0.7)
        repeat(5) { source.update(3.0, it.toLong()) }
        val snapshot = source.read()

        val coordinator = source.create()
        repeat(20) { coordinator.merge(snapshot) }

        // Every snapshot is identical, so averaging into it is idempotent once the first is adopted.
        assertEquals(snapshot.variance, coordinator.read().variance, 1e-9)
    }

    @Test
    fun `a recursive variance stat still averages once it has data of its own`() {
        val stat = RecursiveVarianceStat(omega = 0.1, alpha = 0.2, beta = 0.7)
        stat.update(1.0, 1L)
        val own = stat.read().variance
        val remote = 100.0

        stat.merge(RecursiveVarianceResult(remote, 0.1, 0.2, 0.7))

        assertEquals(0.5 * (own + remote), stat.read().variance, 1e-9)
    }

    @Test
    fun `merging a seasonal trace brings its phase with its factors`() {
        val a = SeasonalSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4)
        val b = SeasonalSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4)
        repeat(9) { a.update(it.toDouble()) } // leaves a and b on different slots
        repeat(6) { b.update(it.toDouble()) }
        val target = b.read().currentSlot

        a.merge(b.read())

        // The averaged factors came from b, so the slot has to be b's too; keeping a's silently
        // paired the factors with a mismatched phase.
        assertEquals(target, a.read().currentSlot)
    }

    @Test
    fun `a damped forecast far ahead converges instead of stalling`() {
        val stat = HoltStat(alpha = 0.5, beta = 0.5, phi = 0.9)
        stat.update(1.0)
        stat.update(2.0)
        val r = stat.read()

        // Closed form, so a huge horizon is instant; the damped series also has a finite limit.
        val far = r.forecast(Int.MAX_VALUE)
        assertTrue(far.isFinite(), "damped forecast diverged to $far")
        assertEquals(r.level + (0.9 / 0.1) * r.trend, far, 1e-6)
    }

    @Test
    fun `an undamped forecast is still linear in the horizon`() {
        val stat = HoltStat(alpha = 0.5, beta = 0.5)
        stat.update(1.0)
        stat.update(2.0)
        val r = stat.read()

        assertEquals(r.level + 10.0 * r.trend, r.forecast(10), 1e-9)
    }

    @Test
    fun `a seasonal forecast far ahead stays finite`() {
        val stat = SeasonalSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4, phi = 0.9)
        repeat(12) { stat.update(it.toDouble()) }

        assertTrue(stat.read().forecast(1_000_000).isFinite())
    }
}

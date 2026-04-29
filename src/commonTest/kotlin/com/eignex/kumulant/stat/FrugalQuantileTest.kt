package com.eignex.kumulant.stat

import com.eignex.kumulant.core.QuantileResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class FrugalQuantileTest {

    @Test
    fun `estimate moves up when value is above it`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 0.0)
        fq.update(100.0)
        assertTrue(fq.read().quantile > 0.0)
    }

    @Test
    fun `estimate moves down when value is below it`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 100.0)
        fq.update(0.0)
        assertTrue(fq.read().quantile < 100.0)
    }

    @Test
    fun `estimate does not move when value equals it`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 5.0)
        fq.update(5.0)
        assertEquals(5.0, fq.read().quantile)
    }

    @Test
    fun `q=1 estimate only moves up`() {
        val fq = FrugalQuantile(q = 1.0, stepSize = 1.0, initialEstimate = 0.0)
        fq.update(50.0)
        val after = fq.read().quantile
        fq.update(0.0)
        assertEquals(after, fq.read().quantile)
    }

    @Test
    fun `q=0 estimate only moves down`() {
        val fq = FrugalQuantile(q = 0.0, stepSize = 1.0, initialEstimate = 100.0)
        fq.update(0.0)
        val after = fq.read().quantile
        fq.update(200.0)
        assertEquals(after, fq.read().quantile)
    }

    @Test
    fun `estimate stays near median of symmetric alternating stream`() {

        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 50.0)
        repeat(2000) {
            fq.update(0.0)
            fq.update(100.0)
        }

        assertTrue(fq.read().quantile in 40.0..60.0, "quantile=${fq.read().quantile}")
    }

    @Test
    fun `zero weight update is ignored`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 5.0)
        fq.update(100.0, weight = 0.0)
        assertEquals(5.0, fq.read().quantile)
    }

    @Test
    fun `reset returns to initial estimate`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 3.0)
        repeat(100) { fq.update(100.0) }
        fq.reset()
        assertEquals(3.0, fq.read().quantile)
    }

    @Test
    fun `merge averages two estimates`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 20.0)
        fq.merge(QuantileResult(0.5, 40.0))
        assertEquals(30.0, fq.read().quantile)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val fq1 = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 0.0)
        val fq2 = fq1.create()
        repeat(100) { fq2.update(100.0) }
        assertEquals(0.0, fq1.read().quantile)
        assertTrue(fq2.read().quantile > fq1.read().quantile)
    }

    @Test
    fun `invalid q throws`() {
        assertFailsWith<IllegalArgumentException> { FrugalQuantile(q = -0.1) }
        assertFailsWith<IllegalArgumentException> { FrugalQuantile(q = 1.1) }
    }
}

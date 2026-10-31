package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.operation.windowed
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class FrugalQuantileTest {

    @Test
    fun `estimate moves up when value is above it`() {
        val fq = FrugalQuantileStat(q = 0.5, stepSize = 1.0, initialEstimate = 0.0)
        fq.update(100.0)
        assertTrue(fq.read().quantile > 0.0)
    }

    @Test
    fun `estimate moves down when value is below it`() {
        val fq = FrugalQuantileStat(q = 0.5, stepSize = 1.0, initialEstimate = 100.0)
        fq.update(0.0)
        assertTrue(fq.read().quantile < 100.0)
    }

    @Test
    fun `estimate does not move when value equals it`() {
        val fq = FrugalQuantileStat(q = 0.5, stepSize = 1.0, initialEstimate = 5.0)
        fq.update(5.0)
        assertEquals(5.0, fq.read().quantile)
    }

    @Test
    fun `q=1 estimate only moves up`() {
        val fq = FrugalQuantileStat(q = 1.0, stepSize = 1.0, initialEstimate = 0.0)
        fq.update(50.0)
        val after = fq.read().quantile
        fq.update(0.0)
        assertEquals(after, fq.read().quantile)
    }

    @Test
    fun `q=0 estimate only moves down`() {
        val fq = FrugalQuantileStat(q = 0.0, stepSize = 1.0, initialEstimate = 100.0)
        fq.update(0.0)
        val after = fq.read().quantile
        fq.update(200.0)
        assertEquals(after, fq.read().quantile)
    }

    @Test
    fun `estimate stays near median of symmetric alternating stream`() {
        val fq = FrugalQuantileStat(q = 0.5, stepSize = 1.0, initialEstimate = 50.0)
        repeat(2000) {
            fq.update(0.0)
            fq.update(100.0)
        }

        assertTrue(fq.read().quantile in 40.0..60.0, "quantile=${fq.read().quantile}")
    }

    @Test
    fun `reset returns to initial estimate`() {
        val fq = FrugalQuantileStat(q = 0.5, stepSize = 1.0, initialEstimate = 3.0)
        repeat(100) { fq.update(100.0) }
        fq.reset()
        assertEquals(3.0, fq.read().quantile)
    }

    @Test
    fun `merge averages two estimates`() {
        val fq = FrugalQuantileStat(q = 0.5, stepSize = 1.0, initialEstimate = 20.0)
        fq.update(20.0)
        fq.merge(QuantileResult(0.5, 40.0))
        assertEquals(30.0, fq.read().quantile)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val fq1 = FrugalQuantileStat(q = 0.5, stepSize = 1.0, initialEstimate = 0.0)
        val fq2 = fq1.create()
        repeat(100) { fq2.update(100.0) }
        assertEquals(0.0, fq1.read().quantile)
        assertTrue(fq2.read().quantile > fq1.read().quantile)
    }

    @Test
    fun `invalid q throws`() {
        assertFailsWith<IllegalArgumentException> { FrugalQuantileStat(q = -0.1) }
        assertFailsWith<IllegalArgumentException> { FrugalQuantileStat(q = 1.1) }
    }

    @Test
    fun `merge into a fresh estimator adopts the incoming estimate`() {
        val shard = FrugalQuantileStat(q = 0.5, stepSize = 1.0)
        repeat(200) { shard.update(100.0) }
        val aggregate = shard.create()
        aggregate.merge(shard.read())
        assertEquals(shard.read().quantile, aggregate.read().quantile)
    }

    @Test
    fun `merging an empty snapshot keeps the estimate`() {
        val s = FrugalQuantileStat(q = 0.5, stepSize = 1.0)
        repeat(200) { s.update(100.0) }
        val before = s.read()
        s.merge(s.create().read())
        assertEquals(before, s.read())
    }

    @Test
    fun `windowed estimate matches the single slice estimate`() {
        val windowed = FrugalQuantileStat(q = 0.5, stepSize = 1.0).windowed(10.seconds, 4)
        val plain = FrugalQuantileStat(q = 0.5, stepSize = 1.0)
        repeat(200) {
            windowed.update(100.0, 1_000L)
            plain.update(100.0)
        }
        assertEquals(plain.read().quantile, windowed.read(1_000L).quantile)
    }
}

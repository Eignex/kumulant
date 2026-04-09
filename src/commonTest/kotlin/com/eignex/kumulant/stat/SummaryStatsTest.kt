package com.eignex.kumulant.stat

import com.eignex.kumulant.core.WeightedMeanResult
import com.eignex.kumulant.core.WeightedVarianceResult
import kotlin.test.*

private const val DELTA = 1e-12

class SumTest {
    @Test
    fun `create produces fresh independent stat`() {
        val s1 = Sum().apply { update(10.0) }
        val s2 = s1.create()   // creates a fresh empty instance
        s1.update(5.0)
        assertEquals(15.0, s1.sum, DELTA)
        assertEquals(0.0, s2.sum, DELTA)
    }

    @Test
    fun `read result carries name`() {
        val s = Sum(name = "total")
        s.update(1.0)
        assertEquals("total", s.read().name)
    }

    @Test
    fun `test extreme values`() {
        val sum = Sum()
        sum.update(1e15, 1.0)
        sum.update(1.0, 1.0)
        assertEquals(1000000000000001.0, sum.sum, 0.1)
    }

    @Test
    fun `test negative weights and values`() {
        val sum = Sum()
        sum.update(-10.0, 1.0)
        sum.update(10.0, -1.0) // -10 + (-10)
        assertEquals(-20.0, sum.sum, DELTA)
    }

    @Test
    fun `test merge logic`() {
        val s1 = Sum().apply { update(10.0, 1.0) }
        val s2 = Sum().apply { update(20.0, 1.0) }
        s1.merge(s2.read())
        assertEquals(30.0, s1.sum, DELTA)
    }

    @Test
    fun `test reset`() {
        val sum = Sum()
        sum.update(100.0)
        sum.reset()
        assertEquals(0.0, sum.sum, DELTA)
    }
}

class MeanTest {
    @Test
    fun `create produces fresh independent stat`() {
        val m1 = Mean().apply { update(10.0) }
        val m2 = m1.create()   // creates a fresh empty instance
        m1.update(20.0)
        assertEquals(15.0, m1.mean, DELTA)
        assertEquals(0.0, m2.totalWeights, DELTA)
    }

    @Test
    fun `read result carries name`() {
        val m = Mean(name = "avg")
        m.update(5.0)
        assertEquals("avg", m.read().name)
    }

    @Test
    fun `test stability with large offset`() {
        val mean = Mean()
        val offset = 1e9
        // Testing that the algorithm handles data far from zero
        mean.update(offset + 1, 1.0)
        mean.update(offset + 2, 1.0)
        mean.update(offset + 3, 1.0)
        assertEquals(offset + 2.0, mean.mean, DELTA)
    }

    @Test
    fun `test zero weight updates`() {
        val mean = Mean()
        mean.update(10.0, 1.0)
        mean.update(100.0, 0.0) // Should not change mean
        assertEquals(10.0, mean.mean, DELTA)
        assertEquals(1.0, mean.totalWeights, DELTA)
    }

    @Test
    fun `test weighted balance`() {
        val mean = Mean()
        mean.update(10.0, 90.0) // 90% weight
        mean.update(100.0, 10.0) // 10% weight
        assertEquals(19.0, mean.mean, DELTA)
    }

    @Test
    fun `test empty merge`() {
        val mean = Mean()
        mean.update(5.0, 1.0)
        mean.merge(WeightedMeanResult(0.0, 100.0, "mean"))
        assertEquals(5.0, mean.mean, DELTA)
    }

    @Test
    fun `test negative values`() {
        val mean = Mean()
        mean.update(-10.0)
        mean.update(-20.0)
        assertEquals(-15.0, mean.mean, DELTA)
    }

    @Test
    fun `test reset`() {
        val mean = Mean()
        mean.update(50.0)
        mean.reset()
        assertEquals(0.0, mean.mean, DELTA)
        assertEquals(0.0, mean.totalWeights, DELTA)
    }
}

class VarianceTest {
    @Test
    fun `create produces fresh independent stat`() {
        val v1 = Variance().apply { update(10.0); update(20.0) }
        val v2 = v1.create()   // creates a fresh empty instance
        v1.update(30.0)
        assertEquals(3.0, v1.totalWeights, DELTA)
        assertEquals(0.0, v2.totalWeights, DELTA)
    }

    @Test
    fun `read result carries name`() {
        val v = Variance(name = "spread")
        v.update(1.0); v.update(2.0)
        assertEquals("spread", v.read().name)
    }

    @Test
    fun `test variance sequence`() {
        val vari = Variance()
        (1..10).forEach { vari.update(it.toDouble(), 1.0) }
        assertEquals(8.25, vari.variance, DELTA)
    }

    @Test
    fun `test variance methods`() {
        val vari = Variance()
        vari.update(10.0, 1.0)
        vari.update(20.0, 1.0)
        assertEquals(2.0, vari.totalWeights, DELTA)
        assertEquals(15.0, vari.mean, DELTA)
        assertEquals(50.0, vari.sst, DELTA)
        assertEquals(25.0, vari.variance, DELTA)
        assertEquals(5.0, vari.stdDev, DELTA)

        assertEquals(vari.read().totalWeights, vari.totalWeights, DELTA)
        assertEquals(vari.read().mean, vari.mean, DELTA)
        assertEquals(vari.read().sst, vari.sst, DELTA)
        assertEquals(vari.read().variance, vari.variance, DELTA)
        assertEquals(vari.read().stdDev, vari.stdDev, DELTA)
    }

    @Test
    fun `test single value variance`() {
        val vari = Variance()
        vari.update(10.0, 1.0)
        assertTrue(vari.variance.isNaN() || vari.variance == 0.0)
    }

    @Test
    fun `test zero variance`() {
        val vari = Variance()
        repeat(10) { vari.update(5.0) }
        assertEquals(5.0, vari.mean, DELTA)
        assertEquals(0.0, vari.variance, DELTA)
    }

    @Test
    fun `test merge`() {
        val v1 =
            Variance().apply { (1..5).forEach { update(it.toDouble(), 1.0) } }
        val v2 =
            Variance().apply { (6..10).forEach { update(it.toDouble(), 1.0) } }

        v1.merge(v2.read())
        assertEquals(8.25, v1.variance, DELTA)
        assertEquals(5.5, v1.mean, DELTA)
    }

    @Test
    fun `test empty merge`() {
        val v1 = Variance()
        v1.update(1.0)
        v1.merge(Variance().read())
        assertEquals(1.0, v1.totalWeights, DELTA)
    }

    @Test
    fun `test reset`() {
        val vari = Variance()
        vari.update(10.0)
        vari.update(20.0)
        vari.reset()
        assertEquals(0.0, vari.totalWeights, DELTA)
        assertEquals(0.0, vari.mean, DELTA)
        assertEquals(0.0, vari.variance, DELTA)
    }
}

class MomentsTest {
    private val delta = 1e-9

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = Moments().apply { update(1.0); update(2.0); update(3.0) }
        val m2 = m1.create()   // creates a fresh empty instance
        m1.update(4.0)
        assertEquals(4.0, m1.totalWeights, delta)
        assertEquals(0.0, m2.totalWeights, delta)
    }

    @Test
    fun `read result carries name`() {
        val m = Moments(name = "dist")
        m.update(5.0)
        assertEquals("dist", m.read().name)
    }

    @Test
    fun `test skewness for symmetric distribution`() {
        val stat = Moments()
        // Symmetric distribution: Skewness should be 0
        val data = listOf(1.0, 2.0, 3.0, 4.0, 5.0)
        data.forEach { stat.update(it, 1.0) }

        assertEquals(3.0, stat.mean, delta)
        assertEquals(0.0, stat.skewness, delta)
    }

    @Test
    fun `test positive skewness`() {
        val stat = Moments()
        // Right-skewed data
        val data = listOf(1.0, 1.0, 1.0, 2.0, 10.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.skewness > 0.0)
    }

    @Test
    fun `test negative skewness`() {
        val stat = Moments()
        // Left-skewed data
        val data = listOf(10.0, 10.0, 10.0, 9.0, 1.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.skewness < 0.0)
    }

    @Test
    fun `test kurtosis of normal-ish distribution`() {
        val stat = Moments()
        val data = listOf(-2.0, -1.0, 0.0, 1.0, 2.0)
        data.forEach { stat.update(it, 1.0) }

        // Excess Kurtosis for this small flat-ish set will be negative (Platykurtic)
        assertTrue(stat.kurtosis < 0.0)
    }

    @Test
    fun `test leptokurtic distribution`() {
        val stat = Moments()
        // Heavy tails (Leptokurtic) -> high peak, extreme outliers
        repeat(100) { stat.update(0.0, 1.0) }
        stat.update(100.0, 1.0)
        stat.update(-100.0, 1.0)
        assertTrue(stat.kurtosis > 0.0)
    }

    @Test
    fun `test complex merge`() {
        val m1 =
            Moments().apply { listOf(10.0, 12.0).forEach { update(it, 1.0) } }
        val m2 =
            Moments().apply { listOf(100.0, 120.0).forEach { update(it, 1.0) } }

        m1.merge(m2.read())

        assertEquals(60.5, m1.mean, delta)
        assertEquals(4.0, m1.totalWeights, delta)
    }

    @Test
    fun `test reset`() {
        val stat = Moments()
        stat.update(10.0)
        stat.update(20.0)
        stat.reset()

        assertEquals(0.0, stat.totalWeights, delta)
        assertEquals(0.0, stat.mean, delta)
        assertEquals(0.0, stat.variance, delta)
        assertEquals(0.0, stat.skewness, delta)
        assertEquals(0.0, stat.kurtosis, delta)
    }
}

class RollingStatsTest {
    private val delta = 1e-9

    @Test
    fun `RollingMean create produces fresh independent stat`() {
        val m1 = RollingMean(alpha = 0.5).apply { update(10.0) }
        val m2 = m1.create()   // creates a fresh empty instance
        repeat(10) { m1.update(10.0) }
        assertEquals(0.0, m2.mean, delta)
        assertTrue(m1.mean > 0.0)
    }

    @Test
    fun `RollingMean read result carries name`() {
        val m = RollingMean(alpha = 0.5, name = "ema")
        m.update(5.0)
        assertEquals("ema", m.read().name)
    }

    @Test
    fun `RollingVariance create produces fresh independent stat`() {
        val v1 = RollingVariance(alpha = 0.5).apply { update(1.0); update(2.0) }
        val v2 = v1.create()   // creates a fresh empty instance
        repeat(10) { v1.update(1000.0) }
        assertEquals(0.0, v2.totalWeights, delta)
        assertTrue(v1.totalWeights > 0.0)
    }

    @Test
    fun `RollingVariance read result carries name`() {
        val v = RollingVariance(alpha = 0.5, name = "vol")
        v.update(1.0); v.update(2.0)
        assertEquals("vol", v.read().name)
    }

    @Test
    fun `RollingMean merge behavior`() {
        val d1 = RollingMean(alpha = 0.5)
        d1.update(10.0)
        val d2 = WeightedMeanResult(1.0, 20.0)

        d1.merge(d2)
        // (10 + 20) / 2 = 15
        assertEquals(15.0, d1.mean, delta)
    }

    @Test
    fun `RollingMean biases toward heavy recent values`() {
        val stat = RollingMean(alpha = 0.5)
        stat.update(10.0, 1.0)
        stat.update(100.0, 10.0) // Massive recent update

        // A simple mean would be ~91.8, but a heavily decayed mean
        // will aggressively track the latest dense value.
        assertTrue(
            stat.mean > 80.0,
            "Mean should heavily favor the massive recent update"
        )
    }

    @Test
    fun `RollingVariance tracking volatility shift`() {
        val stat = RollingVariance(alpha = 0.1)

        // Phase 1: Low variance
        repeat(50) { stat.update(10.0, 1.0) }
        val lowVar = stat.variance

        // Phase 2: Massive spike
        stat.update(1000.0, 1.0)
        val highVar = stat.variance

        assertTrue(highVar > lowVar, "Variance should spike on outlier")
    }

    @Test
    fun `RollingVariance empty merge`() {
        val stat = RollingVariance(alpha = 0.1)
        stat.update(10.0, 1.0)
        stat.update(20.0, 1.0)
        val currentVar = stat.variance

        // Merge with zero-weight remote
        stat.merge(WeightedVarianceResult(0.0, 0.0, 0.0))

        assertEquals(currentVar, stat.variance, delta)
    }

    @Test
    fun `RollingVariance bias correction prevents zero division`() {
        val stat = RollingVariance(alpha = 0.1)
        // Should return 0.0 or something sensible, not NaN, before updates
        assertEquals(0.0, stat.mean, delta)
        assertEquals(0.0, stat.variance, delta)
    }

    @Test
    fun `test reset for decaying stats`() {
        val meanStat = RollingMean(alpha = 0.5)
        meanStat.update(10.0)
        meanStat.reset()
        assertEquals(0.0, meanStat.mean, delta)

        val varStat = RollingVariance(alpha = 0.5)
        varStat.update(10.0)
        varStat.update(20.0)
        varStat.reset()
        assertEquals(0.0, varStat.mean, delta)
        assertEquals(0.0, varStat.variance, delta)
        assertEquals(0.0, varStat.totalWeights, delta)
    }
}

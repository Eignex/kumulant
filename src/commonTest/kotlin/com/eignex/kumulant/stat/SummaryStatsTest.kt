package com.eignex.kumulant.stat

import com.eignex.kumulant.core.WeightedMeanResult
import com.eignex.kumulant.core.WeightedVarianceResult
import kotlin.test.*

private const val DELTA = 1e-12

class SumTest {
    @Test
    fun `create produces fresh independent stat`() {
        val s1 = Sum().apply { update(10.0) }
        val s2 = s1.create() // creates a fresh empty instance
        s1.update(5.0)
        assertEquals(15.0, s1.read().sum, DELTA)
        assertEquals(0.0, s2.read().sum, DELTA)
    }

    @Test
    fun `test extreme values`() {
        val sum = Sum()
        sum.update(1e15, 1.0)
        sum.update(1.0, 1.0)
        assertEquals(1000000000000001.0, sum.read().sum, 0.1)
    }

    @Test
    fun `test negative weights and values`() {
        val sum = Sum()
        sum.update(-10.0, 1.0)
        sum.update(10.0, -1.0) // -10 + (-10)
        assertEquals(-20.0, sum.read().sum, DELTA)
    }

    @Test
    fun `test merge logic`() {
        val s1 = Sum().apply { update(10.0, 1.0) }
        val s2 = Sum().apply { update(20.0, 1.0) }
        s1.merge(s2.read())
        assertEquals(30.0, s1.read().sum, DELTA)
    }

    @Test
    fun `test reset`() {
        val sum = Sum()
        sum.update(100.0)
        sum.reset()
        assertEquals(0.0, sum.read().sum, DELTA)
    }
}

class MeanTest {
    @Test
    fun `create produces fresh independent stat`() {
        val m1 = Mean().apply { update(10.0) }
        val m2 = m1.create() // creates a fresh empty instance
        m1.update(20.0)
        assertEquals(15.0, m1.read().mean, DELTA)
        assertEquals(0.0, m2.read().totalWeights, DELTA)
    }

    @Test
    fun `test stability with large offset`() {
        val mean = Mean()
        val offset = 1e9
        // Testing that the algorithm handles data far from zero
        mean.update(offset + 1, 1.0)
        mean.update(offset + 2, 1.0)
        mean.update(offset + 3, 1.0)
        assertEquals(offset + 2.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test zero weight updates`() {
        val mean = Mean()
        mean.update(10.0, 1.0)
        mean.update(100.0, 0.0) // Should not change mean
        assertEquals(10.0, mean.read().mean, DELTA)
        assertEquals(1.0, mean.read().totalWeights, DELTA)
    }

    @Test
    fun `test weighted balance`() {
        val mean = Mean()
        mean.update(10.0, 90.0) // 90% weight
        mean.update(100.0, 10.0) // 10% weight
        assertEquals(19.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test empty merge`() {
        val mean = Mean()
        mean.update(5.0, 1.0)
        mean.merge(WeightedMeanResult(0.0, 100.0))
        assertEquals(5.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test negative values`() {
        val mean = Mean()
        mean.update(-10.0)
        mean.update(-20.0)
        assertEquals(-15.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test reset`() {
        val mean = Mean()
        mean.update(50.0)
        mean.reset()
        assertEquals(0.0, mean.read().mean, DELTA)
        assertEquals(0.0, mean.read().totalWeights, DELTA)
    }
}

class VarianceTest {
    @Test
    fun `create produces fresh independent stat`() {
        val v1 = Variance().apply {
            update(10.0)
            update(20.0)
        }
        val v2 = v1.create() // creates a fresh empty instance
        v1.update(30.0)
        assertEquals(3.0, v1.read().totalWeights, DELTA)
        assertEquals(0.0, v2.read().totalWeights, DELTA)
    }

    @Test
    fun `test variance sequence`() {
        val vari = Variance()
        (1..10).forEach { vari.update(it.toDouble(), 1.0) }
        assertEquals(8.25, vari.read().variance, DELTA)
    }

    @Test
    fun `test variance methods`() {
        val vari = Variance()
        vari.update(10.0, 1.0)
        vari.update(20.0, 1.0)
        val result = vari.read()
        assertEquals(2.0, result.totalWeights, DELTA)
        assertEquals(15.0, result.mean, DELTA)
        assertEquals(50.0, result.sst, DELTA)
        assertEquals(25.0, result.variance, DELTA)
        assertEquals(5.0, result.stdDev, DELTA)
    }

    @Test
    fun `test single value variance`() {
        val vari = Variance()
        vari.update(10.0, 1.0)
        assertTrue(vari.read().variance.isNaN() || vari.read().variance == 0.0)
    }

    @Test
    fun `test zero variance`() {
        val vari = Variance()
        repeat(10) { vari.update(5.0) }
        assertEquals(5.0, vari.read().mean, DELTA)
        assertEquals(0.0, vari.read().variance, DELTA)
    }

    @Test
    fun `test merge`() {
        val v1 =
            Variance().apply { (1..5).forEach { update(it.toDouble(), 1.0) } }
        val v2 =
            Variance().apply { (6..10).forEach { update(it.toDouble(), 1.0) } }

        v1.merge(v2.read())
        assertEquals(8.25, v1.read().variance, DELTA)
        assertEquals(5.5, v1.read().mean, DELTA)
    }

    @Test
    fun `test empty merge`() {
        val v1 = Variance()
        v1.update(1.0)
        v1.merge(Variance().read())
        assertEquals(1.0, v1.read().totalWeights, DELTA)
    }

    @Test
    fun `test reset`() {
        val vari = Variance()
        vari.update(10.0)
        vari.update(20.0)
        vari.reset()
        assertEquals(0.0, vari.read().totalWeights, DELTA)
        assertEquals(0.0, vari.read().mean, DELTA)
        assertEquals(0.0, vari.read().variance, DELTA)
    }
}

class MomentsTest {
    private val delta = 1e-9

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = Moments().apply {
            update(1.0)
            update(2.0)
            update(3.0)
        }
        val m2 = m1.create() // creates a fresh empty instance
        m1.update(4.0)
        assertEquals(4.0, m1.read().totalWeights, delta)
        assertEquals(0.0, m2.read().totalWeights, delta)
    }

    @Test
    fun `test skewness for symmetric distribution`() {
        val stat = Moments()
        // Symmetric distribution: Skewness should be 0
        val data = listOf(1.0, 2.0, 3.0, 4.0, 5.0)
        data.forEach { stat.update(it, 1.0) }

        assertEquals(3.0, stat.read().mean, delta)
        assertEquals(0.0, stat.read().skewness, delta)
    }

    @Test
    fun `test positive skewness`() {
        val stat = Moments()
        // Right-skewed data
        val data = listOf(1.0, 1.0, 1.0, 2.0, 10.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.read().skewness > 0.0)
    }

    @Test
    fun `test negative skewness`() {
        val stat = Moments()
        // Left-skewed data
        val data = listOf(10.0, 10.0, 10.0, 9.0, 1.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.read().skewness < 0.0)
    }

    @Test
    fun `test kurtosis of normal-ish distribution`() {
        val stat = Moments()
        val data = listOf(-2.0, -1.0, 0.0, 1.0, 2.0)
        data.forEach { stat.update(it, 1.0) }

        // Excess Kurtosis for this small flat-ish set will be negative (Platykurtic)
        assertTrue(stat.read().kurtosis < 0.0)
    }

    @Test
    fun `test leptokurtic distribution`() {
        val stat = Moments()
        // Heavy tails (Leptokurtic) -> high peak, extreme outliers
        repeat(100) { stat.update(0.0, 1.0) }
        stat.update(100.0, 1.0)
        stat.update(-100.0, 1.0)
        assertTrue(stat.read().kurtosis > 0.0)
    }

    @Test
    fun `test complex merge`() {
        val m1 =
            Moments().apply { listOf(10.0, 12.0).forEach { update(it, 1.0) } }
        val m2 =
            Moments().apply { listOf(100.0, 120.0).forEach { update(it, 1.0) } }

        m1.merge(m2.read())

        assertEquals(60.5, m1.read().mean, delta)
        assertEquals(4.0, m1.read().totalWeights, delta)
    }

    @Test
    fun `test reset`() {
        val stat = Moments()
        stat.update(10.0)
        stat.update(20.0)
        stat.reset()

        assertEquals(0.0, stat.read().totalWeights, delta)
        assertEquals(0.0, stat.read().mean, delta)
        assertEquals(0.0, stat.read().variance, delta)
        assertEquals(0.0, stat.read().skewness, delta)
        assertEquals(0.0, stat.read().kurtosis, delta)
    }
}

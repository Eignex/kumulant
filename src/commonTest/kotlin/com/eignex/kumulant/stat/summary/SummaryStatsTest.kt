package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.stat.summary.WeightedMeanResult

import kotlin.math.sqrt
import kotlin.test.*

private const val DELTA = 1e-12

class SumTest {
    @Test
    fun `create produces fresh independent stat`() {
        val s1 = Sum().apply { update(10.0) }
        val s2 = s1.create()
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
        sum.update(10.0, -1.0)
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
        val m2 = m1.create()
        m1.update(20.0)
        assertEquals(15.0, m1.read().mean, DELTA)
        assertEquals(0.0, m2.read().totalWeights, DELTA)
    }

    @Test
    fun `test stability with large offset`() {
        val mean = Mean()
        val offset = 1e9

        mean.update(offset + 1, 1.0)
        mean.update(offset + 2, 1.0)
        mean.update(offset + 3, 1.0)
        assertEquals(offset + 2.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test zero weight updates`() {
        val mean = Mean()
        mean.update(10.0, 1.0)
        mean.update(100.0, 0.0)
        assertEquals(10.0, mean.read().mean, DELTA)
        assertEquals(1.0, mean.read().totalWeights, DELTA)
    }

    @Test
    fun `test weighted balance`() {
        val mean = Mean()
        mean.update(10.0, 90.0)
        mean.update(100.0, 10.0)
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
        val v2 = v1.create()
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
        val m2 = m1.create()
        m1.update(4.0)
        assertEquals(4.0, m1.read().totalWeights, delta)
        assertEquals(0.0, m2.read().totalWeights, delta)
    }

    @Test
    fun `test skewness for symmetric distribution`() {
        val stat = Moments()

        val data = listOf(1.0, 2.0, 3.0, 4.0, 5.0)
        data.forEach { stat.update(it, 1.0) }

        assertEquals(3.0, stat.read().mean, delta)
        assertEquals(0.0, stat.read().skewness, delta)
    }

    @Test
    fun `test positive skewness`() {
        val stat = Moments()

        val data = listOf(1.0, 1.0, 1.0, 2.0, 10.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.read().skewness > 0.0)
    }

    @Test
    fun `test negative skewness`() {
        val stat = Moments()

        val data = listOf(10.0, 10.0, 10.0, 9.0, 1.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.read().skewness < 0.0)
    }

    @Test
    fun `test kurtosis of normal-ish distribution`() {
        val stat = Moments()
        val data = listOf(-2.0, -1.0, 0.0, 1.0, 2.0)
        data.forEach { stat.update(it, 1.0) }

        assertTrue(stat.read().kurtosis < 0.0)
    }

    @Test
    fun `test leptokurtic distribution`() {
        val stat = Moments()

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

class SampleVarianceTraitTest {
    private val delta = 1e-9

    @Test
    fun `sampleVariance applies Bessel correction`() {
        val stat = Moments()
        listOf(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        assertEquals(5.0, r.mean, delta)
        assertEquals(4.0, r.variance, delta)
        assertEquals(32.0 / 7.0, r.sampleVariance, delta)
        assertEquals(sqrt(32.0 / 7.0), r.sampleStdDev, delta)
    }

    @Test
    fun `sampleVariance is zero when totalWeights le 1`() {
        val empty = Moments().read()
        assertEquals(0.0, empty.sampleVariance, delta)
        assertEquals(0.0, empty.sampleStdDev, delta)

        val one = Moments().apply { update(42.0, 1.0) }.read()
        assertEquals(0.0, one.sampleVariance, delta)
        assertEquals(0.0, one.sampleStdDev, delta)
    }

    @Test
    fun `unbiasedSkewness is zero with two or fewer samples`() {
        val empty = Moments().read()
        assertEquals(0.0, empty.unbiasedSkewness, delta)

        val two = Moments().apply {
            update(1.0, 1.0)
            update(3.0, 1.0)
        }.read()
        assertEquals(0.0, two.unbiasedSkewness, delta)
    }

    @Test
    fun `unbiasedSkewness scales biased skewness by sample-size factor`() {
        val stat = Moments()
        listOf(1.0, 1.0, 1.0, 2.0, 10.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        val n = r.totalWeights
        val expected = (sqrt(n * (n - 1)) / (n - 2)) * r.skewness
        assertEquals(expected, r.unbiasedSkewness, delta)
        assertTrue(r.unbiasedSkewness > r.skewness)
    }

    @Test
    fun `unbiasedKurtosis is zero with three or fewer samples`() {
        val three = Moments().apply {
            update(1.0, 1.0)
            update(2.0, 1.0)
            update(3.0, 1.0)
        }.read()
        assertEquals(0.0, three.unbiasedKurtosis, delta)
    }

    @Test
    fun `unbiasedKurtosis matches the algebraic definition`() {
        val stat = Moments()
        listOf(-2.0, -1.0, 0.0, 1.0, 2.0, 3.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        val n = r.totalWeights
        val expected = ((n - 1) / ((n - 2) * (n - 3))) * ((n + 1) * r.kurtosis + 6.0)
        assertEquals(expected, r.unbiasedKurtosis, delta)
    }
}

class VarianceEdgeCasesTest {

    @Test
    fun `read before any update returns zero variance and zero mean`() {
        val v = Variance().read()
        assertEquals(0.0, v.totalWeights, DELTA)
        assertEquals(0.0, v.mean, DELTA)
        assertEquals(0.0, v.variance, DELTA)
    }

    @Test
    fun `variance over constant stream is zero`() {
        val v = Variance()
        repeat(100) { v.update(7.0) }
        assertEquals(0.0, v.read().variance, 1e-6)
    }

    @Test
    fun `handles large magnitudes without overflow`() {
        val v = Variance()
        val large = 1e9
        v.update(large)
        v.update(-large)
        val result = v.read()
        assertFalse(result.variance.isNaN())
        assertFalse(result.variance.isInfinite())
        assertEquals(1e18, result.variance, 1e12)
    }
}

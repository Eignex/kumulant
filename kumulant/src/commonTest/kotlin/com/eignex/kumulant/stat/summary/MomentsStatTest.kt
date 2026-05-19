package com.eignex.kumulant.stat.summary

import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MomentsStatTest {
    private val delta = 1e-9

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = MomentsStat().apply {
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
        val stat = MomentsStat()

        val data = listOf(1.0, 2.0, 3.0, 4.0, 5.0)
        data.forEach { stat.update(it, 1.0) }

        assertEquals(3.0, stat.read().mean, delta)
        assertEquals(0.0, stat.read().skewness, delta)
    }

    @Test
    fun `test positive skewness`() {
        val stat = MomentsStat()

        val data = listOf(1.0, 1.0, 1.0, 2.0, 10.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.read().skewness > 0.0)
    }

    @Test
    fun `test negative skewness`() {
        val stat = MomentsStat()

        val data = listOf(10.0, 10.0, 10.0, 9.0, 1.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.read().skewness < 0.0)
    }

    @Test
    fun `test kurtosis of normal-ish distribution`() {
        val stat = MomentsStat()
        val data = listOf(-2.0, -1.0, 0.0, 1.0, 2.0)
        data.forEach { stat.update(it, 1.0) }

        assertTrue(stat.read().kurtosis < 0.0)
    }

    @Test
    fun `test leptokurtic distribution`() {
        val stat = MomentsStat()

        repeat(100) { stat.update(0.0, 1.0) }
        stat.update(100.0, 1.0)
        stat.update(-100.0, 1.0)
        assertTrue(stat.read().kurtosis > 0.0)
    }

    @Test
    fun `test complex merge`() {
        val m1 =
            MomentsStat().apply { listOf(10.0, 12.0).forEach { update(it, 1.0) } }
        val m2 =
            MomentsStat().apply { listOf(100.0, 120.0).forEach { update(it, 1.0) } }

        m1.merge(m2.read())

        assertEquals(60.5, m1.read().mean, delta)
        assertEquals(4.0, m1.read().totalWeights, delta)
    }

    @Test
    fun `test reset`() {
        val stat = MomentsStat()
        stat.update(10.0)
        stat.update(20.0)
        stat.reset()

        assertEquals(0.0, stat.read().totalWeights, delta)
        assertEquals(0.0, stat.read().mean, delta)
        assertEquals(0.0, stat.read().variance, delta)
        assertEquals(0.0, stat.read().skewness, delta)
        assertEquals(0.0, stat.read().kurtosis, delta)
    }

    @Test
    fun `sampleVariance applies Bessel correction`() {
        val stat = MomentsStat()
        listOf(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        assertEquals(5.0, r.mean, delta)
        assertEquals(4.0, r.variance, delta)
        assertEquals(32.0 / 7.0, r.sampleVariance, delta)
        assertEquals(sqrt(32.0 / 7.0), r.sampleStdDev, delta)
    }

    @Test
    fun `sampleVariance is zero when totalWeights le 1`() {
        val empty = MomentsStat().read()
        assertEquals(0.0, empty.sampleVariance, delta)
        assertEquals(0.0, empty.sampleStdDev, delta)

        val one = MomentsStat().apply { update(42.0, 1.0) }.read()
        assertEquals(0.0, one.sampleVariance, delta)
        assertEquals(0.0, one.sampleStdDev, delta)
    }

    @Test
    fun `unbiasedSkewness is zero with two or fewer samples`() {
        val empty = MomentsStat().read()
        assertEquals(0.0, empty.unbiasedSkewness, delta)

        val two = MomentsStat().apply {
            update(1.0, 1.0)
            update(3.0, 1.0)
        }.read()
        assertEquals(0.0, two.unbiasedSkewness, delta)
    }

    @Test
    fun `unbiasedSkewness scales biased skewness by sample-size factor`() {
        val stat = MomentsStat()
        listOf(1.0, 1.0, 1.0, 2.0, 10.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        val n = r.totalWeights
        val expected = (sqrt(n * (n - 1)) / (n - 2)) * r.skewness
        assertEquals(expected, r.unbiasedSkewness, delta)
        assertTrue(r.unbiasedSkewness > r.skewness)
    }

    @Test
    fun `unbiasedKurtosis is zero with three or fewer samples`() {
        val three = MomentsStat().apply {
            update(1.0, 1.0)
            update(2.0, 1.0)
            update(3.0, 1.0)
        }.read()
        assertEquals(0.0, three.unbiasedKurtosis, delta)
    }

    @Test
    fun `unbiasedKurtosis matches the algebraic definition`() {
        val stat = MomentsStat()
        listOf(-2.0, -1.0, 0.0, 1.0, 2.0, 3.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        val n = r.totalWeights
        val expected = ((n - 1) / ((n - 2) * (n - 3))) * ((n + 1) * r.kurtosis + 6.0)
        assertEquals(expected, r.unbiasedKurtosis, delta)
    }
}

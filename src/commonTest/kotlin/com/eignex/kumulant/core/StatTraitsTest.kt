package com.eignex.kumulant.core

import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class StatTraitsTest {

    private data class Rate(override val rate: Double) : HasRate

    @Test
    fun `HasRate per rescales to the requested duration`() {
        val r = Rate(2.0)
        assertEquals(2.0, r.per(1.seconds), absoluteTolerance = 1e-12)
        assertEquals(120.0, r.per(1.minutes), absoluteTolerance = 1e-12)
    }

    private data class Sv(
        override val totalWeights: Double,
        override val sst: Double,
    ) : HasSampleVariance

    @Test
    fun `HasSampleVariance returns zero when totalWeights is zero`() {
        val v = Sv(0.0, 0.0)
        assertEquals(0.0, v.variance)
        assertEquals(0.0, v.stdDev)
        assertEquals(0.0, v.sampleVariance)
        assertEquals(0.0, v.sampleStdDev)
    }

    @Test
    fun `HasSampleVariance population vs sample variance differ by Bessel factor`() {
        val v = Sv(totalWeights = 10.0, sst = 90.0)
        assertEquals(9.0, v.variance, absoluteTolerance = 1e-12)
        assertEquals(10.0, v.sampleVariance, absoluteTolerance = 1e-12)
        assertEquals(sqrt(9.0), v.stdDev, absoluteTolerance = 1e-12)
        assertEquals(sqrt(10.0), v.sampleStdDev, absoluteTolerance = 1e-12)
    }

    @Test
    fun `HasSampleVariance sample variance falls back to zero with n_le_1`() {
        assertEquals(0.0, Sv(0.5, 1.0).sampleVariance)
        assertEquals(0.0, Sv(1.0, 1.0).sampleVariance)
    }

    private data class Shape(
        override val totalWeights: Double,
        override val sst: Double,
        override val m3: Double,
        override val m4: Double,
    ) : HasShapeMoments

    @Test
    fun `HasShapeMoments skewness and kurtosis vanish without variance or weight`() {
        val empty = Shape(totalWeights = 0.0, sst = 0.0, m3 = 0.0, m4 = 0.0)
        assertEquals(0.0, empty.skewness)
        assertEquals(0.0, empty.kurtosis)
        assertEquals(0.0, empty.unbiasedSkewness)
        assertEquals(0.0, empty.unbiasedKurtosis)
    }

    @Test
    fun `HasShapeMoments produces nonzero skewness and kurtosis with data`() {
        val s = Shape(totalWeights = 10.0, sst = 10.0, m3 = 5.0, m4 = 50.0)
        assertTrue(s.skewness != 0.0)
        assertTrue(s.kurtosis != 0.0)
    }

    @Test
    fun `HasShapeMoments unbiased skewness applies n adjustment factor`() {
        val s = Shape(totalWeights = 10.0, sst = 10.0, m3 = 5.0, m4 = 30.0)
        val biased = s.skewness
        val unbiased = s.unbiasedSkewness
        assertTrue(biased != 0.0 && unbiased != 0.0)
        assertTrue(unbiased > biased)
    }

    @Test
    fun `HasShapeMoments unbiased kurtosis falls back when n_le_3`() {
        assertEquals(0.0, Shape(3.0, 3.0, 1.0, 5.0).unbiasedKurtosis)
        val s = Shape(totalWeights = 5.0, sst = 5.0, m3 = 0.0, m4 = 15.0)
        assertTrue(s.unbiasedKurtosis != 0.0)
    }

    @Test
    fun `HasShapeMoments m2 mirrors sst`() {
        val s = Shape(totalWeights = 4.0, sst = 12.0, m3 = 0.0, m4 = 0.0)
        assertEquals(12.0, s.m2)
    }

    private data class Lin(override val slope: Double, override val intercept: Double) : HasLinearModel

    @Test
    fun `HasLinearModel predict applies slope and intercept`() {
        val m = Lin(slope = 2.0, intercept = -1.0)
        assertEquals(-1.0, m.predict(0.0))
        assertEquals(9.0, m.predict(5.0))
    }

    private data class Reg(
        override val totalWeights: Double,
        override val sst: Double,
        override val sse: Double,
    ) : HasRegression

    @Test
    fun `HasRegression derives metrics from sst and sse`() {
        val r = Reg(totalWeights = 10.0, sst = 100.0, sse = 25.0)
        assertEquals(75.0, r.ssr)
        assertEquals(2.5, r.mse)
        assertEquals(sqrt(2.5), r.rmse, absoluteTolerance = 1e-12)
        assertEquals(0.75, r.rSquared, absoluteTolerance = 1e-12)
    }

    @Test
    fun `HasRegression returns zero metrics on empty inputs`() {
        val empty = Reg(totalWeights = 0.0, sst = 0.0, sse = 0.0)
        assertEquals(0.0, empty.mse)
        assertEquals(0.0, empty.rmse)
        assertEquals(0.0, empty.rSquared)
    }
}

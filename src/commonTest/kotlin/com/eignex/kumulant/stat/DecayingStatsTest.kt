package com.eignex.kumulant.stat

import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-9

private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T2 = 3_000_000_000L
private const val T3 = 11_000_000_000L

class DecayingSumTest {

    @Test
    fun `sum is positive after update`() {
        val s = DecayingSum(halfLife = 1.seconds)
        s.update(10.0, T0)
        assertTrue(s.read(T0).sum > 0.0)
    }

    @Test
    fun `sum at update time equals value`() {
        val s = DecayingSum(halfLife = 1.seconds)
        s.update(5.0, T0)

        assertEquals(5.0, s.read(T0).sum, 1e-9)
    }

    @Test
    fun `sum decays to half after one half-life`() {
        val s = DecayingSum(halfLife = 1.seconds)
        s.update(8.0, T0)
        val sumAfterHalfLife = s.read(T1).sum
        assertEquals(4.0, sumAfterHalfLife, 1e-9)
    }

    @Test
    fun `sum decays toward zero far in the future`() {
        val s = DecayingSum(halfLife = 1.seconds)
        s.update(1.0, T0)
        val sumNear = s.read(T1).sum
        val sumFar = s.read(T3).sum
        assertTrue(sumFar < sumNear / 100.0)
    }

    @Test
    fun `accumulates multiple updates`() {
        val s = DecayingSum(halfLife = 1.seconds)
        s.update(3.0, T0)
        s.update(4.0, T0)
        assertEquals(7.0, s.read(T0).sum, 1e-9)
    }

    @Test
    fun `merge combines two sums`() {
        val s1 = DecayingSum(halfLife = 1.seconds)
        val s2 = DecayingSum(halfLife = 1.seconds)
        s1.update(3.0, T0)
        s2.update(4.0, T0)
        s1.merge(s2.read(T0))
        assertEquals(7.0, s1.read(T0).sum, 1e-9)
    }

    @Test
    fun `reset yields zero sum`() {
        val s = DecayingSum(halfLife = 1.seconds)
        s.update(5.0, T0)
        s.reset()
        assertEquals(0.0, s.read(T1).sum, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val s1 = DecayingSum(halfLife = 1.seconds)
        s1.update(10.0, T0)
        val s2 = s1.create()
        s2.update(10.0, T1)
        assertTrue(s1.read(T2).sum < s2.read(T2).sum)
    }
}

class DecayingMeanTest {

    @Test
    fun `mean of constant stream equals that constant`() {
        val m = DecayingMean(halfLife = 1.seconds)
        repeat(10) { m.update(7.0, T0) }
        assertEquals(7.0, m.read(T0).mean, 1e-9)
    }

    @Test
    fun `mean shifts toward recent values`() {
        val m = DecayingMean(halfLife = 1.seconds)

        repeat(100) { m.update(0.0, T0) }

        repeat(100) { m.update(100.0, T3) }
        val mean = m.read(T3).mean

        assertTrue(mean > 99.0, "mean=$mean should be near 100 after old values decayed")
    }

    @Test
    fun `weighted update influences mean proportionally`() {
        val m = DecayingMean(halfLife = 1.seconds)
        m.update(0.0, T0, weight = 3.0)
        m.update(10.0, T0, weight = 1.0)
        val mean = m.read(T0).mean
        assertEquals(2.5, mean, 1e-9)
    }

    @Test
    fun `totalWeights halves after one half-life`() {
        val m = DecayingMean(halfLife = 1.seconds)
        m.update(1.0, T0)
        val countNow = m.read(T0).totalWeights
        val countLater = m.read(T1).totalWeights
        assertEquals(countNow / 2.0, countLater, 1e-9)
    }

    @Test
    fun `merge combines two independent streams`() {
        val m1 = DecayingMean(halfLife = 1.seconds)
        val m2 = DecayingMean(halfLife = 1.seconds)
        repeat(10) { m1.update(0.0, T0) }
        repeat(10) { m2.update(10.0, T0) }
        m1.merge(m2.read(T0))
        assertEquals(5.0, m1.read(T0).mean, 1e-9)
    }

    @Test
    fun `reset yields zero mean and count`() {
        val m = DecayingMean(halfLife = 1.seconds)
        m.update(42.0, T0)
        m.reset()
        val r = m.read(T1)
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.totalWeights, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = DecayingMean(halfLife = 1.seconds)
        m1.update(5.0, T0)
        val m2 = m1.create()
        repeat(100) { m2.update(100.0, T1) }
        assertTrue(m2.read(T1).mean > m1.read(T1).mean)
    }
}

class DecayingVarianceTest {

    @Test
    fun `variance of constant stream is zero`() {
        val v = DecayingVariance(halfLife = 1.seconds)
        repeat(100) { v.update(5.0, T0) }
        assertEquals(0.0, v.read(T0).variance, 1e-9)
        assertEquals(5.0, v.read(T0).mean, 1e-9)
    }

    @Test
    fun `variance of two equal-weight values`() {
        val v = DecayingVariance(halfLife = 1.seconds)
        v.update(0.0, T0)
        v.update(10.0, T0)
        val r = v.read(T0)

        assertEquals(5.0, r.mean, 1e-9)
        assertEquals(25.0, r.variance, 1e-9)
    }

    @Test
    fun `variance increases when signal disperses`() {
        val v = DecayingVariance(halfLife = 1.seconds)

        repeat(20) { v.update(5.0, T0) }
        val varTight = v.read(T0).variance

        repeat(20) { i -> v.update(i.toDouble(), T3) }
        val varSpread = v.read(T3).variance
        assertTrue(varSpread > varTight)
    }

    @Test
    fun `stdDev is sqrt of variance`() {
        val v = DecayingVariance(halfLife = 1.seconds)
        v.update(0.0, T0)
        v.update(10.0, T0)
        val r = v.read(T0)
        assertEquals(sqrt(r.variance), r.stdDev, 1e-9)
    }

    @Test
    fun `merge combines two streams`() {
        val v1 = DecayingVariance(halfLife = 1.seconds)
        val v2 = DecayingVariance(halfLife = 1.seconds)
        repeat(10) { v1.update(0.0, T0) }
        repeat(10) { v2.update(10.0, T0) }
        v1.merge(v2.read(T0))

        assertEquals(5.0, v1.read(T0).mean, 1e-9)
        assertEquals(25.0, v1.read(T0).variance, 1e-9)
    }

    @Test
    fun `reset clears mean and variance`() {
        val v = DecayingVariance(halfLife = 1.seconds)
        v.update(1.0, T0)
        v.update(9.0, T0)
        v.reset()
        val r = v.read(T1)
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.variance, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val v1 = DecayingVariance(halfLife = 1.seconds)
        v1.update(5.0, T0)
        val v2 = v1.create()
        repeat(50) { v2.update(100.0, T1) }

        assertTrue(v2.read(T1).mean > v1.read(T1).mean)
    }
}

class DecayingRateTest {

    @Test
    fun `rate is positive after updates`() {
        val r = DecayingRate(halfLife = 1.seconds)
        r.update(1.0, T0)
        r.update(1.0, T1)
        assertTrue(r.read(T2).rate > 0.0)
    }

    @Test
    fun `rate decays toward zero far in the future`() {
        val r = DecayingRate(halfLife = 1.seconds)
        r.update(1.0, T0)
        val rateNear = r.read(T1).rate
        val rateFar = r.read(T3).rate
        assertTrue(rateFar < rateNear / 100.0, "rate should decay significantly over 10 half-lives")
    }

    @Test
    fun `reset yields zero rate`() {
        val r = DecayingRate(halfLife = 1.seconds)
        r.update(1.0, T0)
        r.reset()
        assertEquals(0.0, r.read(T1).rate, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val r1 = DecayingRate(halfLife = 1.seconds)
        r1.update(10.0, T0)
        val r2 = r1.create()
        r2.update(100.0, T1)

        val rate1 = r1.read(T2).rate
        val rate2 = r2.read(T2).rate
        assertTrue(rate2 > rate1, "r2 should have higher rate after extra update")
    }
}

package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.sqrt
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-9
private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T2 = 3_000_000_000L
private const val T3 = 11_000_000_000L

class EwmaMeanTest {
    private val delta = 1e-9

    @Test
    fun `EwmaMean create produces fresh independent stat`() {
        val m1 = EwmaMean(alpha = 0.5).apply { update(10.0) }
        val m2 = m1.create()
        repeat(10) { m1.update(10.0) }
        assertEquals(0.0, m2.read().mean, delta)
        assertTrue(m1.read().mean > 0.0)
    }

    @Test
    fun `EwmaMean merge behavior`() {
        val d1 = EwmaMean(alpha = 0.5)
        d1.update(10.0)
        val d2 = WeightedMeanResult(1.0, 20.0)

        d1.merge(d2)

        assertEquals(15.0, d1.read().mean, delta)
    }

    @Test
    fun `EwmaMean biases toward heavy recent values`() {
        val stat = EwmaMean(alpha = 0.5)
        stat.update(10.0, 1.0)
        stat.update(100.0, 10.0)

        assertTrue(
            stat.read().mean > 80.0,
            "Mean should heavily favor the massive recent update"
        )
    }

    @Test
    fun `EwmaMean reset clears state`() {
        val meanStat = EwmaMean(alpha = 0.5)
        meanStat.update(10.0)
        meanStat.reset()
        assertEquals(0.0, meanStat.read().mean, delta)
    }
}

class EwmaVarianceTest {
    private val delta = 1e-9

    @Test
    fun `EwmaVariance create produces fresh independent stat`() {
        val v1 = EwmaVariance(alpha = 0.5).apply {
            update(1.0)
            update(2.0)
        }
        val v2 = v1.create()
        repeat(10) { v1.update(1000.0) }
        assertEquals(0.0, v2.read().totalWeights, delta)
        assertTrue(v1.read().totalWeights > 0.0)
    }

    @Test
    fun `EwmaVariance tracking volatility shift`() {
        val stat = EwmaVariance(alpha = 0.1)

        repeat(50) { stat.update(10.0, 1.0) }
        val lowVar = stat.read().variance

        stat.update(1000.0, 1.0)
        val highVar = stat.read().variance

        assertTrue(highVar > lowVar, "Variance should spike on outlier")
    }

    @Test
    fun `EwmaVariance empty merge`() {
        val stat = EwmaVariance(alpha = 0.1)
        stat.update(10.0, 1.0)
        stat.update(20.0, 1.0)
        val currentVar = stat.read().variance

        stat.merge(WeightedVarianceResult(0.0, 0.0, 0.0))

        assertEquals(currentVar, stat.read().variance, delta)
    }

    @Test
    fun `EwmaVariance bias correction prevents zero division`() {
        val stat = EwmaVariance(alpha = 0.1)

        assertEquals(0.0, stat.read().mean, delta)
        assertEquals(0.0, stat.read().variance, delta)
    }

    @Test
    fun `EwmaVariance reset clears state`() {
        val varStat = EwmaVariance(alpha = 0.5)
        varStat.update(10.0)
        varStat.update(20.0)
        varStat.reset()
        assertEquals(0.0, varStat.read().mean, delta)
        assertEquals(0.0, varStat.read().variance, delta)
        assertEquals(0.0, varStat.read().totalWeights, delta)
    }
}

class EwmaEdgeCasesTest {

    @Test
    fun `EwmaMean before any update returns zero total weight and zero mean`() {
        val m = EwmaMean(alpha = 0.1)
        val r = m.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.mean, DELTA)
    }

    @Test
    fun `EwmaVariance before any update returns zero state`() {
        val v = EwmaVariance(alpha = 0.1)
        val r = v.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.variance, DELTA)
    }
}

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

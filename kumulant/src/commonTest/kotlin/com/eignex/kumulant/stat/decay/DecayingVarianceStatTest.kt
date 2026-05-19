package com.eignex.kumulant.stat.decay

import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-9
private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T3 = 11_000_000_000L

class DecayingVarianceStatTest {

    @Test
    fun `variance of constant stream is zero`() {
        val v = DecayingVarianceStat(halfLife = 1.seconds)
        repeat(100) { v.update(5.0, T0) }
        assertEquals(0.0, v.read(T0).variance, 1e-9)
        assertEquals(5.0, v.read(T0).mean, 1e-9)
    }

    @Test
    fun `variance of two equal-weight values`() {
        val v = DecayingVarianceStat(halfLife = 1.seconds)
        v.update(0.0, T0)
        v.update(10.0, T0)
        val r = v.read(T0)
        assertEquals(5.0, r.mean, 1e-9)
        assertEquals(25.0, r.variance, 1e-9)
    }

    @Test
    fun `variance increases when signal disperses`() {
        val v = DecayingVarianceStat(halfLife = 1.seconds)
        repeat(20) { v.update(5.0, T0) }
        val varTight = v.read(T0).variance
        repeat(20) { i -> v.update(i.toDouble(), T3) }
        val varSpread = v.read(T3).variance
        assertTrue(varSpread > varTight)
    }

    @Test
    fun `stdDev is sqrt of variance`() {
        val v = DecayingVarianceStat(halfLife = 1.seconds)
        v.update(0.0, T0)
        v.update(10.0, T0)
        val r = v.read(T0)
        assertEquals(sqrt(r.variance), r.stdDev, 1e-9)
    }

    @Test
    fun `merge combines two streams`() {
        val v1 = DecayingVarianceStat(halfLife = 1.seconds)
        val v2 = DecayingVarianceStat(halfLife = 1.seconds)
        repeat(10) { v1.update(0.0, T0) }
        repeat(10) { v2.update(10.0, T0) }
        v1.merge(v2.read(T0))
        assertEquals(5.0, v1.read(T0).mean, 1e-9)
        assertEquals(25.0, v1.read(T0).variance, 1e-9)
    }

    @Test
    fun `reset clears mean and variance`() {
        val v = DecayingVarianceStat(halfLife = 1.seconds)
        v.update(1.0, T0)
        v.update(9.0, T0)
        v.reset()
        val r = v.read(T1)
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.variance, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val v1 = DecayingVarianceStat(halfLife = 1.seconds)
        v1.update(5.0, T0)
        val v2 = v1.create()
        repeat(50) { v2.update(100.0, T1) }
        assertTrue(v2.read(T1).mean > v1.read(T1).mean)
    }
}

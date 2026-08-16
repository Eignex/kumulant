package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T3 = 11_000_000_000L

class DecayingMeanStatTest {

    @Test
    fun `mean of constant stream equals that constant`() {
        val m = DecayingMeanStat(halfLife = 1.seconds)
        repeat(10) { m.update(7.0, T0) }
        assertEquals(7.0, m.read(T0).mean, 1e-9)
    }

    @Test
    fun `mean shifts toward recent values`() {
        val m = DecayingMeanStat(halfLife = 1.seconds)
        repeat(100) { m.update(0.0, T0) }
        repeat(100) { m.update(100.0, T3) }
        val mean = m.read(T3).mean
        assertTrue(mean > 99.0, "mean=$mean should be near 100 after old values decayed")
    }

    @Test
    fun `weighted update influences mean proportionally`() {
        val m = DecayingMeanStat(halfLife = 1.seconds)
        m.update(0.0, T0, weight = 3.0)
        m.update(10.0, T0, weight = 1.0)
        val mean = m.read(T0).mean
        assertEquals(2.5, mean, 1e-9)
    }

    @Test
    fun `totalWeights halves after one half-life`() {
        val m = DecayingMeanStat(halfLife = 1.seconds)
        m.update(1.0, T0)
        val countNow = m.read(T0).totalWeights
        val countLater = m.read(T1).totalWeights
        assertEquals(countNow / 2.0, countLater, 1e-9)
    }

    @Test
    fun `merge combines two independent streams`() {
        val m1 = DecayingMeanStat(halfLife = 1.seconds)
        val m2 = DecayingMeanStat(halfLife = 1.seconds)
        repeat(10) { m1.update(0.0, T0) }
        repeat(10) { m2.update(10.0, T0) }
        m1.merge(m2.read(T0))
        assertEquals(5.0, m1.read(T0).mean, 1e-9)
    }

    @Test
    fun `reset yields zero mean and count`() {
        val m = DecayingMeanStat(halfLife = 1.seconds)
        m.update(42.0, T0)
        m.reset()
        val r = m.read(T1)
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.totalWeights, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = DecayingMeanStat(halfLife = 1.seconds)
        m1.update(5.0, T0)
        val m2 = m1.create()
        repeat(100) { m2.update(100.0, T1) }
        assertTrue(m2.read(T1).mean > m1.read(T1).mean)
    }
}

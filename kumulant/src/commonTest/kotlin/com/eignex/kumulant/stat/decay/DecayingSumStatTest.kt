package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T2 = 3_000_000_000L
private const val T3 = 11_000_000_000L

class DecayingSumStatTest {

    @Test
    fun `sum is positive after update`() {
        val s = DecayingSumStat(halfLife = 1.seconds)
        s.update(10.0, T0)
        assertTrue(s.read(T0).sum > 0.0)
    }

    @Test
    fun `sum at update time equals value`() {
        val s = DecayingSumStat(halfLife = 1.seconds)
        s.update(5.0, T0)
        assertEquals(5.0, s.read(T0).sum, 1e-9)
    }

    @Test
    fun `sum decays to half after one half-life`() {
        val s = DecayingSumStat(halfLife = 1.seconds)
        s.update(8.0, T0)
        val sumAfterHalfLife = s.read(T1).sum
        assertEquals(4.0, sumAfterHalfLife, 1e-9)
    }

    @Test
    fun `sum decays toward zero far in the future`() {
        val s = DecayingSumStat(halfLife = 1.seconds)
        s.update(1.0, T0)
        val sumNear = s.read(T1).sum
        val sumFar = s.read(T3).sum
        assertTrue(sumFar < sumNear / 100.0)
    }

    @Test
    fun `accumulates multiple updates`() {
        val s = DecayingSumStat(halfLife = 1.seconds)
        s.update(3.0, T0)
        s.update(4.0, T0)
        assertEquals(7.0, s.read(T0).sum, 1e-9)
    }

    @Test
    fun `merge combines two sums`() {
        val s1 = DecayingSumStat(halfLife = 1.seconds)
        val s2 = DecayingSumStat(halfLife = 1.seconds)
        s1.update(3.0, T0)
        s2.update(4.0, T0)
        s1.merge(s2.read(T0))
        assertEquals(7.0, s1.read(T0).sum, 1e-9)
    }

    @Test
    fun `reset yields zero sum`() {
        val s = DecayingSumStat(halfLife = 1.seconds)
        s.update(5.0, T0)
        s.reset()
        assertEquals(0.0, s.read(T1).sum, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val s1 = DecayingSumStat(halfLife = 1.seconds)
        s1.update(10.0, T0)
        val s2 = s1.create()
        s2.update(10.0, T1)
        assertTrue(s1.read(T2).sum < s2.read(T2).sum)
    }
}

class DecayingSumLandmarkTest {

    // A stream numbered far behind the landmark: the landmark starts at process uptime, so this is
    // what a replay stream numbering from its own epoch looks like once the process has been up
    // longer than ~1075 half-lives. A negative stamp reaches the same offset without the wait.
    private val farBehind = -200_000_000_000L

    @Test
    fun `a stream far behind the landmark reports a finite sum`() {
        val s = DecayingSumStat(halfLife = 100.milliseconds)
        s.update(5.0, timestampNanos = farBehind)
        assertEquals(5.0, s.read(farBehind).sum, DELTA)
    }

    @Test
    fun `a stream far behind the landmark keeps the mean finite`() {
        val s = DecayingMeanStat(halfLife = 100.milliseconds)
        s.update(5.0, timestampNanos = farBehind)
        assertEquals(5.0, s.read(farBehind).mean, DELTA)
    }
}

package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T3 = 11_000_000_000L

class DecayingVarianceStatTest {

    @Test
    fun `stdDev is sqrt of variance`() {
        val v = DecayingVarianceStat(halfLife = 1.seconds)
        v.update(0.0, T0)
        v.update(10.0, T0)
        val r = v.read(T0)
        assertEquals(sqrt(r.variance), r.stdDev, 1e-9)
    }

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
    fun `fresh accumulator adopts a replay snapshot at its timestamp`() {
        for (concurrency in Concurrency.entries) {
            val source = DecayingVarianceStat(1.seconds, concurrency)
            val timestamp = -2_000_000_000_000L
            source.update(4.0, timestamp)
            source.update(6.0, timestamp)
            val snapshot = source.read(timestamp)
            val target = source.create()
            target.merge(snapshot)

            assertEquals(snapshot, target.read(timestamp), "mode=$concurrency")
        }
    }

    @Test
    fun `backdated read reports the landmark timestamp`() {
        val stat = DecayingVarianceStat(1.seconds)
        stat.update(4.0, T3)

        assertEquals(T3, stat.read(T0).timestampNanos)
    }

    @Test
    fun `merging a backdated read preserves the remote evidence weight`() {
        val source = DecayingVarianceStat(1.seconds)
        source.update(4.0, T3)
        source.update(6.0, T3)
        val target = DecayingVarianceStat(1.seconds)
        target.update(5.0, T3)
        target.merge(source.read(T0))

        assertEquals(3.0, target.read(T3).totalWeights, 1e-9)
    }

    @Test
    fun `empty replay reads retain the requested timestamp`() {
        val stat = DecayingVarianceStat(1.seconds)

        assertEquals(0L, stat.read(0L).timestampNanos)
    }
}

package com.eignex.kumulant.operation

import com.eignex.koblas.F64DenseVector
import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.regression.CovarianceStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class SamplingTest {

    @Test
    fun `reset replays the sampling sequence from the start`() {
        // Stat.reset promises the equivalent of a fresh stat, and the gate's draw counter is state:
        // without clearing it a reset stat would carry on mid-sequence.
        val a = CountStat().sample(rate = 0.5, seed = 3L)
        repeat(50) { a.update(1.0) }
        val before = a.read().sum
        a.reset()
        repeat(50) { a.update(1.0) }
        assertEquals(before, a.read().sum, DELTA)
    }

    @Test
    fun `copies sample independently of the stat they came from`() {
        // Window slices are built through create; sharing the parent's seed would make every slice
        // accept and reject at the same positions.
        val template = CountStat().sample(rate = 0.5, seed = 11L)
        val first = template.create()
        val second = template.create()
        repeat(200) {
            first.update(1.0)
            second.update(1.0)
        }
        assertTrue(
            first.read().sum != second.read().sum,
            "copies drew the same sequence: ${first.read().sum}",
        )
    }

    @Test
    fun `series throttle forwards every Nth update`() {
        val stat = CountStat().throttle(every = 3)
        repeat(10) { stat.update(1.0) }
        // 10 inputs at every=3 => updates fire at ticks 3, 6, 9 => 3 forwarded.
        assertEquals(3.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired throttle forwards every Nth update`() {
        val stat = SumStat().atY().throttle(every = 2)
        for (y in doubleArrayOf(1.0, 2.0, 3.0, 4.0, 5.0)) stat.update(0.0, y)
        // y=2 (tick 2) and y=4 (tick 4) make it through; total y = 6.
        assertEquals(6.0, stat.read().sum, DELTA)
    }

    @Test
    fun `discrete throttle forwards every Nth update`() {
        val stat = CountStat().asDiscrete().throttle(every = 4)
        repeat(20) { stat.update(7L) }
        assertEquals(5.0, stat.read().sum, DELTA)
    }

    @Test
    fun `vector throttle forwards every Nth update`() {
        val stat = VectorizedStat(2, SumStat()).throttle(every = 2)
        repeat(6) { stat.update(F64DenseVector.of(doubleArrayOf(1.0, 1.0))) }
        assertEquals(3.0, stat.read().results[0].sum, DELTA)
    }

    @Test
    fun `throttle rejects invalid every`() {
        assertFailsWith<IllegalArgumentException> { CountStat().throttle(every = 0) }
        assertFailsWith<IllegalArgumentException> { CountStat().throttle(every = -1) }
    }

    @Test
    fun `series sample keeps roughly the rate fraction`() {
        val n = 10_000
        val stat = CountStat().sample(rate = 0.3, seed = 42L)
        repeat(n) { stat.update(1.0) }
        val kept = stat.read().sum
        assertTrue(kept in 2_500.0..3_500.0, "expected ~3000, got $kept")
    }

    @Test
    fun `sample rate 0 drops everything and 1 keeps everything`() {
        val none = CountStat().sample(rate = 0.0, seed = 1L)
        val all = CountStat().sample(rate = 1.0, seed = 1L)
        repeat(50) {
            none.update(1.0)
            all.update(1.0)
        }
        assertEquals(0.0, none.read().sum, DELTA)
        assertEquals(50.0, all.read().sum, DELTA)
    }

    @Test
    fun `sample rejects out-of-range rate`() {
        assertFailsWith<IllegalArgumentException> { CountStat().sample(rate = -0.1, seed = 0L) }
        assertFailsWith<IllegalArgumentException> { CountStat().sample(rate = 1.1, seed = 0L) }
    }

    @Test
    fun `series weightBy multiplies caller weight by expression result`() {
        val stat = SumStat().weightBy { v -> v * v }
        // sum += value * weight * weighter(value); caller weight defaults to 1.
        stat.update(2.0) // contributes 2 * 1 * 4 = 8
        stat.update(3.0) // contributes 3 * 1 * 9 = 27
        assertEquals(35.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired weightBy multiplies caller weight by expression`() {
        val stat = SumStat().atY().weightBy { _, y -> y }
        stat.update(0.0, y = 2.0) // 2 * 1 * 2 = 4
        stat.update(0.0, y = 5.0) // 5 * 1 * 5 = 25
        assertEquals(29.0, stat.read().sum, DELTA)
    }

    @Test
    fun `discrete weightBy composes with caller weight`() {
        val stat = SumStat().asDiscrete().weightBy { v -> v.toDouble() }
        stat.update(value = 3L, weight = 2.0) // 3 * 2 * 3 = 18
        assertEquals(18.0, stat.read().sum, DELTA)
    }
}

class ThrottleWindowedTest {

    @Test
    fun `a windowed paired throttle keeps one gate for the whole stream`() {
        val s = CovarianceStat().throttle(every = 10).windowed(duration = 60.seconds, slices = 10)
        for (i in 0 until 60) s.update(1.0 + i, 2.0 + i, timestampNanos = i * 1_000_000_000L)
        assertTrue(s.read(59_000_000_000L).totalWeights > 0.0, "the gate never fired in any slice")
    }

    @Test
    fun `a windowed discrete throttle keeps one gate for the whole stream`() {
        val s = HyperLogLogStat().throttle(every = 10).windowed(duration = 60.seconds, slices = 10)
        for (i in 0 until 60) s.update(i.toLong(), timestampNanos = i * 1_000_000_000L)
        assertTrue(s.read(59_000_000_000L).estimate > 0.0, "the gate never fired in any slice")
    }
}

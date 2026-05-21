package com.eignex.kumulant.operation

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class SamplingTest {

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
        repeat(6) { stat.update(DenseVector.of(doubleArrayOf(1.0, 1.0))) }
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
        val stat = CountStat().sample(rate = 0.3, random = Random(42))
        repeat(n) { stat.update(1.0) }
        val kept = stat.read().sum
        assertTrue(kept in 2_500.0..3_500.0, "expected ~3000, got $kept")
    }

    @Test
    fun `sample rate 0 drops everything and 1 keeps everything`() {
        val none = CountStat().sample(rate = 0.0, random = Random(1))
        val all = CountStat().sample(rate = 1.0, random = Random(1))
        repeat(50) {
            none.update(1.0)
            all.update(1.0)
        }
        assertEquals(0.0, none.read().sum, DELTA)
        assertEquals(50.0, all.read().sum, DELTA)
    }

    @Test
    fun `sample rejects out-of-range rate`() {
        assertFailsWith<IllegalArgumentException> { CountStat().sample(rate = -0.1, random = Random(0)) }
        assertFailsWith<IllegalArgumentException> { CountStat().sample(rate = 1.1, random = Random(0)) }
    }

    @Test
    fun `series weightBy multiplies caller weight by expression result`() {
        val stat = SumStat().weightBy { v -> v * v }
        // sum += value * weight * weighter(value); caller weight defaults to 1.
        stat.update(2.0)  // contributes 2 * 1 * 4 = 8
        stat.update(3.0)  // contributes 3 * 1 * 9 = 27
        assertEquals(35.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired weightBy multiplies caller weight by expression`() {
        val stat = SumStat().atY().weightBy { _, y -> y }
        stat.update(0.0, y = 2.0)  // 2 * 1 * 2 = 4
        stat.update(0.0, y = 5.0)  // 5 * 1 * 5 = 25
        assertEquals(29.0, stat.read().sum, DELTA)
    }

    @Test
    fun `discrete weightBy composes with caller weight`() {
        val stat = SumStat().asDiscrete().weightBy { v -> v.toDouble() }
        stat.update(value = 3L, weight = 2.0)  // 3 * 2 * 3 = 18
        assertEquals(18.0, stat.read().sum, DELTA)
    }
}

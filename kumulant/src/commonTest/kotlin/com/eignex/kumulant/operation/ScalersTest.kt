package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.ops.minMaxScaler
import com.eignex.kumulant.schema.ops.standardScaler
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Mean
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class ScalersTest {

    @Test
    fun `standardScaler emits zero on the first update`() {
        val s = Sum.standardScaler().materialize()
        s.update(5.0)
        assertEquals(0.0, s.read().sum, DELTA)
    }

    @Test
    fun `standardScaler matches hand-rolled z-score per update`() {
        val xs = doubleArrayOf(1.0, 2.0, 3.0, 4.0, 5.0)
        val scaled = Sum.standardScaler().materialize()
        for (x in xs) scaled.update(x)
        val ref = VarianceStat()
        var expected = 0.0
        for (x in xs) {
            ref.update(x)
            val r = ref.read()
            expected += if (r.stdDev == 0.0) 0.0 else (x - r.mean) / r.stdDev
        }
        assertEquals(expected, scaled.read().sum, DELTA)
    }

    @Test
    fun `minMaxScaler maps to unit interval by default`() {
        val s = Sum.minMaxScaler().materialize()
        s.update(2.0)
        s.update(10.0)
        s.update(6.0)
        assertEquals(1.5, s.read().sum, DELTA)
    }

    @Test
    fun `minMaxScaler maps to negative-one positive-one`() {
        val s = Sum.minMaxScaler(targetLow = -1.0, targetHigh = 1.0).materialize()
        s.update(0.0)
        s.update(10.0)
        s.update(5.0)
        s.update(0.0)
        assertEquals(-1.0, s.read().sum, DELTA)
    }

    @Test
    fun `minMaxScaler rejects inverted range`() {
        assertFailsWith<IllegalArgumentException> {
            Sum.minMaxScaler(targetLow = 1.0, targetHigh = 0.0).materialize()
        }
    }

    @Test
    fun `minMaxScaler emits targetLow while the range is degenerate`() {
        val s = Sum.minMaxScaler(targetLow = -3.0, targetHigh = 3.0).materialize()
        s.update(7.0)
        s.update(7.0)
        s.update(7.0)
        assertEquals(-9.0, s.read().sum, DELTA)
    }

    @Test
    fun `reset clears scaler primary state`() {
        val s = Sum.standardScaler().materialize()
        for (x in listOf(1.0, 2.0, 3.0, 4.0)) s.update(x)
        assertTrue(s.read().sum != 0.0)
        s.reset()
        s.update(5.0)
        assertEquals(0.0, s.read().sum, DELTA)
    }
}

class ScalerWindowedTest {

    @Test
    fun `a windowed standard scaler keeps one primary for the whole stream`() {
        val s = Mean.standardScaler().materialize().windowed(duration = 60.seconds, slices = 10)
        for (i in 0 until 6) s.update(100.0 * (i + 1), timestampNanos = i * 10_000_000_000L)
        assertNotEquals(0.0, s.read(50_000_000_000L).mean, "every observation was standardized against a fresh primary")
    }
}

package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ScalersTest {

    @Test
    fun `standardScaler emits zero on the first update`() {
        val s = SumStat().standardScaler()
        s.update(5.0)
        assertEquals(0.0, s.read().sum, DELTA)
    }

    @Test
    fun `standardScaler matches hand-rolled z-score per update`() {
        val xs = doubleArrayOf(1.0, 2.0, 3.0, 4.0, 5.0)
        val scaled = SumStat().standardScaler()
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
        val s = SumStat().minMaxScaler()
        // After the very first update min==max so the projection emits targetLow=0.
        s.update(2.0)
        s.update(10.0)
        // Second update: min=2, max=10, value=10 -> 1.0.
        s.update(6.0)
        // Third update: min=2, max=10, value=6 -> 0.5.
        // Cumulative sum: 0 + 1.0 + 0.5 = 1.5.
        assertEquals(1.5, s.read().sum, DELTA)
    }

    @Test
    fun `minMaxScaler maps to negative-one positive-one`() {
        val s = SumStat().minMaxScaler(targetLow = -1.0, targetHigh = 1.0)
        s.update(0.0)
        s.update(10.0)
        // After two distinct observations: min=0, max=10. Value 10 -> +1.0.
        s.update(5.0)
        // min=0, max=10. Value 5 -> 0.0.
        s.update(0.0)
        // min=0, max=10. Value 0 -> -1.0.
        // Cumulative: targetLow (-1) + 1.0 + 0.0 + (-1.0) = -1.0.
        assertEquals(-1.0, s.read().sum, DELTA)
    }

    @Test
    fun `minMaxScaler rejects inverted range`() {
        assertFailsWith<IllegalArgumentException> {
            SumStat().minMaxScaler(targetLow = 1.0, targetHigh = 0.0)
        }
    }

    @Test
    fun `minMaxScaler emits targetLow while the range is degenerate`() {
        val s = SumStat().minMaxScaler(targetLow = -3.0, targetHigh = 3.0)
        // Three identical observations: min stays equal to max throughout, so each update
        // contributes targetLow = -3.0.
        s.update(7.0)
        s.update(7.0)
        s.update(7.0)
        assertEquals(-9.0, s.read().sum, DELTA)
    }

    @Test
    fun `reset clears scaler primary state`() {
        val s = SumStat().standardScaler()
        for (x in listOf(1.0, 2.0, 3.0, 4.0)) s.update(x)
        assertTrue(s.read().sum != 0.0)
        s.reset()
        s.update(5.0)
        // First post-reset update is again the warm-up zero.
        assertEquals(0.0, s.read().sum, DELTA)
    }
}

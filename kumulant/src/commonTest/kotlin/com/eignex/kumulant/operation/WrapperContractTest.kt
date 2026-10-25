package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.expr.Eq
import com.eignex.kumulant.schema.expr.In
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.spec.ResampleAggregator
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WrapperContractTest {

    @Test
    fun `throttle resets its phase`() {
        val stat = SumStat().throttle(3)
        stat.update(1.0, 1L, 1.0)
        stat.update(1.0, 2L, 1.0)

        stat.reset()
        stat.update(100.0, 3L, 1.0)

        assertEquals(0.0, stat.read(4L).sum, "reset left the throttle mid-phase")
        stat.update(100.0, 4L, 1.0)
        stat.update(100.0, 5L, 1.0)
        assertEquals(100.0, stat.read(6L).sum)
    }

    @Test
    fun `a windowed band reads instead of throwing`() {
        val stat = VarianceStat().band(2.0).windowed(1.seconds, slices = 10, concurrency = Concurrency.None)

        stat.update(1.0, 1_000_000L, 1.0)
        stat.update(3.0, 2_000_000L, 1.0)

        val r = stat.read(2_000_000L)
        assertTrue(r.lower <= r.center && r.center <= r.upper, "band came out inverted: $r")
        assertEquals(2.0, r.center, 1e-9)
    }

    @Test
    fun `an empty windowed band still reads`() {
        val stat = VarianceStat().band(2.0).windowed(1.seconds)

        assertEquals(0.0, stat.read(1_000_000L).center)
    }

    @Test
    fun `resampleByTime honours the caller's weight`() {
        val weighted = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Sum)
        weighted.update(7.0, 10_000_000L, 2.0)
        weighted.update(0.0, 2_000_000_000L, 1.0)

        assertEquals(14.0, weighted.read(2_000_000_000L).sum, 1e-9)
    }

    @Test
    fun `In agrees with Eq on the values IEEE and total order disagree about`() {
        val nan = listOf(Double.NaN)
        val zero = listOf(0.0)

        assertFalse(
            In(X, nan).eval(Double.NaN, 0.0, DoubleArray(0), null),
            "In(X, [NaN]) is unsatisfiable and must not admit NaN",
        )
        assertEquals(
            Eq(X, Const(Double.NaN)).eval(Double.NaN, 0.0, DoubleArray(0), null),
            In(X, nan).eval(Double.NaN, 0.0, DoubleArray(0), null),
        )
        assertTrue(In(X, zero).eval(-0.0, 0.0, DoubleArray(0), null), "-0.0 == 0.0 under IEEE")
        assertEquals(
            Eq(X, Const(0.0)).eval(-0.0, 0.0, DoubleArray(0), null),
            In(X, zero).eval(-0.0, 0.0, DoubleArray(0), null),
        )
    }
}

package com.eignex.kumulant.operation

import com.eignex.kumulant.schema.expr.Center
import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.expr.IfExpr
import com.eignex.kumulant.schema.expr.Scale
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.div
import com.eignex.kumulant.schema.expr.gt
import com.eignex.kumulant.schema.expr.minus
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val DELTA = 1e-9

/** Standard-scaler projection: emits 0 while stdDev is still zero (first update). */
private val standardScalerExpr = IfExpr(Scale gt 0.0, (X - Center) / Scale, Const(0.0))

class FeedbackTest {

    @Test
    fun `standard scaler one-liner matches a hand-rolled reference`() {
        val standardized = SumStat().withFeedback(VarianceStat(), standardScalerExpr)
        val xs = doubleArrayOf(1.0, 2.0, 3.0, 4.0, 5.0)
        for (x in xs) standardized.update(x)
        // Each update sees the running mean/stdDev of the values up to and including itself.
        val ref = VarianceStat()
        var expected = 0.0
        for (x in xs) {
            ref.update(x)
            val r = ref.read()
            expected += if (r.stdDev == 0.0) 0.0 else (x - r.mean) / r.stdDev
        }
        assertEquals(expected, standardized.read().sum, DELTA)
    }

    @Test
    fun `Center requires HasCenterScale primary`() {
        // SumStat's result has no center; the projection should fail at update time.
        val s = SumStat().withFeedback(SumStat(), Center)
        assertFailsWith<IllegalStateException> { s.update(1.0) }
    }

    @Test
    fun `primary exposes its own snapshot through the wrapper`() {
        val stat = SumStat().withFeedback(VarianceStat(), standardScalerExpr)
        for (x in listOf(1.0, 2.0, 3.0, 4.0, 5.0)) stat.update(x)
        @Suppress("UNCHECKED_CAST")
        val wrapper = stat as FeedbackSeriesStat<WeightedVarianceResult, *>
        assertEquals(3.0, wrapper.primary.read().mean, DELTA)
    }

    @Test
    fun `MomentsStat also works as primary via HasCenterScale`() {
        val stat = SumStat().withFeedback(MomentsStat(), standardScalerExpr)
        for (x in listOf(0.0, 1.0, 2.0, 3.0, 4.0)) stat.update(x)
        assertTrue(stat.read().sum.isFinite())
    }

    @Test
    fun `reset clears both primary and inner`() {
        val stat = SumStat().withFeedback(VarianceStat(), standardScalerExpr)
        stat.update(5.0)
        stat.update(7.0)
        stat.reset()
        @Suppress("UNCHECKED_CAST")
        val wrapper = stat as FeedbackSeriesStat<WeightedVarianceResult, *>
        assertEquals(0.0, wrapper.primary.read().totalWeights, DELTA)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = SumStat().withFeedback(VarianceStat(), standardScalerExpr).apply {
            update(1.0)
            update(3.0)
            update(5.0)
        }
        val fresh = tpl.create()
        assertEquals(0.0, fresh.read().sum, DELTA)
        assertTrue(tpl.read().sum != fresh.read().sum)
    }
}

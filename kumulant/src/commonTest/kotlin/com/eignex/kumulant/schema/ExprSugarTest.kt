package com.eignex.kumulant.schema

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.IndexedResult
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.summary.SummaryResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ExprSugarTest {

    @Test
    fun `Switch picks the matching case`() {
        val expr = Switch(
            on = VIndex,
            cases = listOf(
                SwitchCase(0.0, Const(10.0)),
                SwitchCase(2.0, Const(20.0)),
            ),
            otherwise = Const(-1.0),
        )
        val primary = WeightedVarianceResult(totalWeights = 1.0, mean = 0.0, variance = 0.0)
        assertEquals(10.0, expr.eval(0.0, 0.0, DoubleArray(0), IndexedResult(primary, 0)), DELTA)
        assertEquals(20.0, expr.eval(0.0, 0.0, DoubleArray(0), IndexedResult(primary, 2)), DELTA)
        assertEquals(-1.0, expr.eval(0.0, 0.0, DoubleArray(0), IndexedResult(primary, 99)), DELTA)
    }

    @Test
    fun `In tests exact membership`() {
        val expr = In(VIndex, listOf(0.0, 3.0))
        val primary = WeightedVarianceResult(totalWeights = 1.0, mean = 0.0, variance = 0.0)
        assertTrue(expr.eval(0.0, 0.0, DoubleArray(0), IndexedResult(primary, 0)))
        assertTrue(expr.eval(0.0, 0.0, DoubleArray(0), IndexedResult(primary, 3)))
        assertFalse(expr.eval(0.0, 0.0, DoubleArray(0), IndexedResult(primary, 1)))
    }

    @Test
    fun `Standardize matches the manual z-score AST`() {
        val primary = WeightedVarianceResult(totalWeights = 4.0, mean = 5.0, variance = 4.0)
        assertEquals(2.5, Standardize.eval(10.0, 0.0, DoubleArray(0), primary), DELTA)
    }

    @Test
    fun `Standardize emits zero when scale is zero`() {
        val primary = WeightedVarianceResult(totalWeights = 1.0, mean = 7.0, variance = 0.0)
        assertEquals(0.0, Standardize.eval(10.0, 0.0, DoubleArray(0), primary), DELTA)
    }

    @Test
    fun `MinMax maps to the configured range`() {
        val primary = SummaryResult(totalWeights = 4.0, mean = 5.0, variance = 0.0, min = 0.0, max = 10.0)
        assertEquals(0.0, MinMax(-1.0, 1.0).eval(5.0, 0.0, DoubleArray(0), primary), DELTA)
        assertEquals(1.0, MinMax(-1.0, 1.0).eval(10.0, 0.0, DoubleArray(0), primary), DELTA)
        assertEquals(-1.0, MinMax(-1.0, 1.0).eval(0.0, 0.0, DoubleArray(0), primary), DELTA)
    }

    @Test
    fun `MinMax rejects inverted range at construction`() {
        assertFailsWith<IllegalArgumentException> { MinMax(targetLow = 1.0, targetHigh = 0.0) }
    }

    @Test
    fun `Switch In Standardize MinMax compose as flat per-index AST over SummaryStat`() {
        val primary = SummaryResult(totalWeights = 4.0, mean = 5.0, variance = 4.0, min = 0.0, max = 10.0)

        val perIndex = Switch(
            on = VIndex,
            cases = listOf(
                SwitchCase(0.0, Standardize),
                SwitchCase(1.0, MinMax(targetLow = 0.0, targetHigh = 1.0)),
            ),
            otherwise = X,
        )

        assertEquals(2.5, perIndex.eval(10.0, 0.0, DoubleArray(0), IndexedResult(primary, 0)), DELTA)
        assertEquals(1.0, perIndex.eval(10.0, 0.0, DoubleArray(0), IndexedResult(primary, 1)), DELTA)
        assertEquals(42.0, perIndex.eval(42.0, 0.0, DoubleArray(0), IndexedResult(primary, 99)), DELTA)
    }
}

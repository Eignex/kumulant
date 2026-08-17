package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.expr.Center
import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.expr.IfExpr
import com.eignex.kumulant.schema.expr.Scale
import com.eignex.kumulant.schema.expr.VIndex
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.div
import com.eignex.kumulant.schema.expr.eq
import com.eignex.kumulant.schema.expr.gt
import com.eignex.kumulant.schema.expr.minus
import com.eignex.kumulant.schema.expr.times
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class PerIndexFeedbackTest {

    @Test
    fun `VIndex lets the projection branch per coordinate`() {
        // Z-score coordinate 0, pass coordinate 1 through, double coordinate 2.
        val dimensions = 3
        val primary = VectorizedStat(dimensions, VarianceStat())
        val standardize = IfExpr(Scale gt 0.0, (X - Center) / Scale, Const(0.0))
        val perIndexExpr = IfExpr(
            VIndex eq 0.0,
            standardize,
            IfExpr(
                VIndex eq 2.0,
                X * 2.0,
                X,
            ),
        )

        @Suppress("UNCHECKED_CAST")
        val scaled = VectorizedStat(dimensions, SumStat())
            .withFeedback(primary, perIndexExpr)

        val updates = listOf(
            doubleArrayOf(1.0, 10.0, 100.0),
            doubleArrayOf(2.0, 20.0, 200.0),
            doubleArrayOf(3.0, 30.0, 300.0),
            doubleArrayOf(4.0, 40.0, 400.0),
        )
        for (u in updates) scaled.update(u)

        val refVar = VarianceStat()
        var expectedCoord0 = 0.0
        for (u in updates) {
            refVar.update(u[0])
            val r = refVar.read()
            expectedCoord0 += if (r.stdDev == 0.0) 0.0 else (u[0] - r.mean) / r.stdDev
        }
        val expectedCoord1 = updates.sumOf { it[1] } // passthrough
        val expectedCoord2 = updates.sumOf { it[2] } * 2.0 // doubled

        val out = scaled.read().results
        assertEquals(expectedCoord0, out[0].sum, DELTA)
        assertEquals(expectedCoord1, out[1].sum, DELTA)
        assertEquals(expectedCoord2, out[2].sum, DELTA)
    }

    @Test
    fun `VIndex outside a feedback context throws`() {
        val primary = VarianceStat().also {
            it.update(1.0)
            it.update(2.0)
        }
            .read()
        assertFailsWith<IllegalStateException> { VIndex.eval(0.0, 0.0, DoubleArray(0), primary) }
    }

    @Test
    fun `paired feedback exposes axis index via VIndex`() {
        // Paired feedback evaluates the x axis with VIndex=0 and the y axis with VIndex=1.
        val pair = com.eignex.kumulant.stat.summary.PairedSumStat().withFeedback(
            VarianceStat(),
            VarianceStat(),
            VIndex,
        )
        pair.update(99.0, 123.0)
        val r = pair.read()
        assertEquals(0.0, r.sumX, DELTA)
        assertEquals(1.0, r.sumY, DELTA)
    }
}

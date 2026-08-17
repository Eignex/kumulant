package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.isNaN
import com.eignex.kumulant.schema.expr.not
import com.eignex.kumulant.schema.ops.filter
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.DDSketch
import kotlin.test.Test
import kotlin.test.assertEquals

// A NaN has no rank, so no bin belongs to it: it falls through the sign comparisons in `update` into
// the zero bucket and drags every quantile toward zero. See `Stat` for why the library propagates a
// NaN value rather than silently discarding it.
class DDSketchNaNTest {

    @Test
    fun `a NaN counts as an observation and lands in the zero bucket`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))

        listOf(10.0, 20.0, 30.0).forEach { sketch.update(it) }
        sketch.update(Double.NaN)

        val r = sketch.read()
        assertEquals(4.0, r.totalWeights, 0.0, "a NaN is a real observation and is counted")
        assertEquals(1.0, r.zeroCount, 0.0, "with no bin of its own, a NaN falls into the zero bucket")
    }

    @Test
    fun `filtering NaN upstream keeps the sketch clean`() {
        // The supported remedy, and the reason IsNaN exists as an expression node: none of the
        // comparison nodes can single out a NaN, since every IEEE comparison against one is false.
        val filtered = DDSketch(relativeError = 0.01, probabilities = listOf(0.5))
            .filter(!X.isNaN())
            .materialize()

        listOf(10.0, 20.0, 30.0).forEach { filtered.update(it, 0L, 1.0) }
        filtered.update(Double.NaN, 0L, 1.0)

        val r = filtered.read(0L)
        assertEquals(3.0, r.totalWeights, 0.0, "the filter must drop the NaN before the sketch sees it")
        assertEquals(0.0, r.zeroCount, 0.0, "and nothing must reach the zero bucket")
    }

    @Test
    fun `a NaN weight is a no-op even though a NaN value is not`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))

        listOf(10.0, 20.0, 30.0).forEach { sketch.update(it) }
        sketch.update(15.0, 0L, Double.NaN)

        assertEquals(3.0, sketch.read().totalWeights, 0.0, "a NaN weight carries no observation")
    }
}

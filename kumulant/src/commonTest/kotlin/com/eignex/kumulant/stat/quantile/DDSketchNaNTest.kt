package com.eignex.kumulant.stat.quantile

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * NaN has no rank, so it cannot be binned. It used to fall through the sign comparisons in
 * `update` into the zero bucket and be counted as an observation of zero, which drags every
 * quantile toward zero. [LinearHistogramStat] already dropped it; this makes DDSketch agree.
 */
class DDSketchNaNTest {

    @Test
    fun `NaN is dropped rather than counted as zero`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        sketch.update(Double.NaN)
        val empty = sketch.read()
        assertEquals(0.0, empty.totalWeights, 0.0, "NaN should not register as an observation")
        assertEquals(0.0, empty.zeroCount, 0.0, "NaN should not land in the zero bucket")

        listOf(10.0, 20.0, 30.0).forEach { sketch.update(it) }
        sketch.update(Double.NaN)
        val r = sketch.read()
        assertEquals(3.0, r.totalWeights, 0.0, "NaN should leave the observation count alone")
        assertEquals(0.0, r.zeroCount, 0.0)
    }
}

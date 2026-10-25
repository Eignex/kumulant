package com.eignex.kumulant.stat.anomaly

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.quantile.FrugalQuantileStat
import com.eignex.kumulant.stat.quantile.QuantileResult
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.summary.MadResult
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class AnomalyScoresTest {

    @Test
    fun `absolute score measures deviation in population standard deviations`() {
        val result = WeightedVarianceResult(totalWeights = 4.0, mean = 10.0, variance = 4.0)

        for ((value, expected) in listOf(6.0 to 2.0, 10.0 to 0.0, 14.0 to 2.0)) {
            assertEquals(expected, result.absoluteZScore(value), DELTA)
        }
    }

    @Test
    fun `absolute score supports robust center and scale`() {
        val result = MadResult(median = 10.0, mad = 2.0)

        assertEquals(2.0, result.absoluteZScore(14.0), DELTA)
    }

    @Test
    fun `absolute score is zero for degenerate scales`() {
        for (variance in listOf(0.0, 1e-26, 1e-24, Double.NaN)) {
            val result = WeightedVarianceResult(totalWeights = 1.0, mean = 7.0, variance = variance)

            assertEquals(0.0, result.absoluteZScore(100.0))
        }
    }

    @Test
    fun `absolute score uses merged summary evidence`() {
        val local = VarianceStat().apply { update(1.0) }
        val remote = VarianceStat().apply { update(3.0, weight = 3.0) }

        local.merge(remote.read())

        assertEquals(3.0, local.read().absoluteZScore(2.5 + 3.0 * kotlin.math.sqrt(0.75)), DELTA)
    }

    @Test
    fun `single quantile scores only strict exceedances`() {
        val result = QuantileResult(probability = 0.9, quantile = 10.0)

        for ((value, expected) in listOf(9.0 to 0.0, 10.0 to 0.0, 11.0 to 1.0, Double.NaN to 0.0)) {
            assertEquals(expected, result.exceedanceScore(value))
        }
    }

    @Test
    fun `frugal snapshots support threshold scoring`() {
        val stat = FrugalQuantileStat(q = 0.9)
        repeat(10) { stat.update(7.0) }
        val result = stat.read()

        assertEquals(1.0, result.exceedanceScore(result.quantile + 1.0))
    }

    @Test
    fun `sketch and digest scores select the requested probability`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.9, 0.5))
        val digest = TDigestStat(probabilities = doubleArrayOf(0.9, 0.5))
        for (i in 1..100) {
            sketch.update(i.toDouble())
            digest.update(i.toDouble())
        }

        for (score in listOf(sketch.read()::exceedanceScore, digest.read()::exceedanceScore)) {
            assertEquals(1.0, score(70.0, 0.5))
            assertEquals(0.0, score(70.0, 0.9))
        }
    }

    @Test
    fun `sketch and digest scores are zero at their threshold`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.9)).apply { update(7.0) }.read()
        val digest = TDigestStat(probabilities = doubleArrayOf(0.9)).apply { update(7.0) }.read()

        assertEquals(0.0, sketch.exceedanceScore(sketch.quantiles[0], 0.9))
        assertEquals(0.0, digest.exceedanceScore(digest.quantiles[0], 0.9))
    }

    @Test
    fun `unconfigured probabilities are rejected`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.9)).read()
        val digest = TDigestStat(probabilities = doubleArrayOf(0.9)).read()

        for (score in listOf(sketch::exceedanceScore, digest::exceedanceScore)) {
            val failure = assertFailsWith<IllegalArgumentException> { score(7.0, 0.5) }

            assertTrue(failure.message.orEmpty().contains("probability=0.5"))
        }
    }

    @Test
    fun `empty quantile snapshots score zero`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.9)).read()
        val digest = TDigestStat(probabilities = doubleArrayOf(0.9)).read()

        assertEquals(0.0, sketch.exceedanceScore(100.0, 0.9))
        assertEquals(0.0, digest.exceedanceScore(100.0, 0.9))
        assertEquals(0.0, QuantileResult(0.9, Double.NaN).exceedanceScore(100.0))
    }
}

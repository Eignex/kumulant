package com.eignex.kumulant.core

import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.quantile.HdrHistogramStat
import com.eignex.kumulant.stat.quantile.LinearHistogramStat
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.score.AucStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.MadStat
import com.eignex.kumulant.stat.summary.MaxStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MinStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// The empty-read convention: report the identity element where the statistic has one, and `NaN` where
// it does not, because a `0.0` quantile is indistinguishable from a sketch that observed real zeros.
class EmptyReadSemanticsTest {

    @Test
    fun `statistics with an identity element report it`() {
        // A sum of nothing really is zero, and a count of nothing really is zero.
        assertEquals(0.0, SumStat().read().sum)
        assertEquals(0.0, CountStat().read().sum, "an empty count is genuinely zero")
        // The infinities are the correct starting points for a fold and cannot be mistaken for
        // observed data.
        assertEquals(Double.POSITIVE_INFINITY, MinStat().read().min)
        assertEquals(Double.NEGATIVE_INFINITY, MaxStat().read().max)
    }

    @Test
    fun `array-backed histograms report no buckets`() {
        assertEquals(0, HdrHistogramStat().read().weights.size)
        assertEquals(0, LinearHistogramStat(0.0, 10.0, 4).read().weights.size)
    }

    @Test
    fun `quantile estimators report NaN rather than a plausible zero`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.5, 0.99)).read()
        assertTrue(sketch.quantiles.all { it.isNaN() }, "DDSketch reported ${sketch.quantiles.toList()}")

        val digest = TDigestStat(probabilities = doubleArrayOf(0.5, 0.99)).read()
        assertTrue(digest.quantiles.all { it.isNaN() }, "TDigest reported ${digest.quantiles.toList()}")

        // MadStat is built on two TDigests, so it inherits the sentinel: an empty sample has no median.
        val mad = MadStat().read()
        assertTrue(mad.median.isNaN(), "Mad median was ${mad.median}")
        assertTrue(mad.mad.isNaN(), "Mad mad was ${mad.mad}")

        assertTrue(AucStat().read().auc.isNaN(), "Auc should already have been NaN")
    }

    @Test
    fun `isEmpty is the same check whatever the count is called`() {
        // totalWeights, totalSeen and totalWeight are three spellings of the same thing across
        // the catalogue; HasObservationCount normalises them.
        val empties: List<HasObservationCount> = listOf(
            MeanStat().read(),
            DDSketchStat().read(),
            TDigestStat().read(),
        )
        for (r in empties) assertTrue(r.isEmpty, "${r::class.simpleName} should report isEmpty")

        val populated: List<HasObservationCount> = listOf(
            MeanStat().apply { update(1.0) }.read(),
            DDSketchStat().apply { update(1.0) }.read(),
            TDigestStat().apply { update(1.0) }.read(),
        )
        for (r in populated) assertTrue(!r.isEmpty, "${r::class.simpleName} should not report isEmpty")
    }

    @Test
    fun `a populated quantile estimator reports finite quantiles again`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.5))
        sketch.update(42.0)
        assertTrue(sketch.read().quantiles[0].isFinite(), "the sentinel must not persist past the first observation")
        sketch.reset()
        assertTrue(sketch.read().quantiles[0].isNaN(), "reset returns the sketch to empty")
    }
}

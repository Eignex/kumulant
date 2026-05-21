package com.eignex.kumulant.operation

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.stat.regression.StochasticRegressionStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.tree.ThresholdSplit
import com.eignex.kumulant.stat.tree.TreeConfig
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12
private fun feat(vararg xs: Double) = DenseVector.of(xs)

class RegressionOpsTest {

    @Test
    fun `filter drops updates failing the predicate`() {
        val inner = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList())
        val stat = inner.filter { _, y -> y > 0.0 }
        stat.update(feat(0.5), y = -1.0)
        stat.update(feat(0.5), y = 1.0)
        stat.update(feat(0.5), y = 2.0)
        assertEquals(2.0, stat.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `transformY rewrites y before update`() {
        val inner = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList())
        val stat = inner.transformY { _, y -> y * 2.0 }
        stat.update(feat(0.0), y = 3.0)
        val snap = stat.read(0L)
        // y was doubled before the inner stat saw it, so mean = 6.0.
        assertEquals(6.0, snap.rootMean, DELTA)
    }

    @Test
    fun `transformX rewrites the feature vector`() {
        val inner = StochasticRegressionStat(featureSize = 2)
        val stat = inner.transformX { v, _ -> doubleArrayOf(v[0] * 2.0, v[1] + 1.0) }
        // Drive a few updates and verify featureSize is unchanged.
        repeat(3) { stat.update(feat(1.0, 2.0), y = 5.0) }
        assertEquals(2, stat.featureSize)
    }

    @Test
    fun `withWeight replaces caller weight`() {
        val inner = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList())
        val stat = inner.withWeight(2.5)
        stat.update(feat(0.0), y = 1.0, weight = 100.0)
        assertEquals(2.5, stat.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `weightBy multiplies caller weight by per-update expr`() {
        val inner = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList())
        val stat = inner.weightBy { _, y -> y * y }
        stat.update(feat(0.0), y = 2.0) // weight = 1 * 4
        stat.update(feat(0.0), y = 3.0) // weight = 1 * 9
        assertEquals(13.0, stat.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `throttle forwards every Nth update`() {
        val inner = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList())
        val stat = inner.throttle(every = 4)
        repeat(20) { stat.update(feat(0.0), y = 1.0) }
        assertEquals(5.0, stat.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `sample keeps roughly the rate fraction`() {
        val n = 10_000
        val inner = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList())
        val stat = inner.sample(rate = 0.25, random = Random(7))
        repeat(n) { stat.update(feat(0.0), y = 1.0) }
        val kept = stat.read(0L).totalWeights
        assertTrue(kept in 2_000.0..3_000.0, "expected ~2500, got $kept")
    }

    @Test
    fun `foldRegression lifts a series stat to consume x and y`() {
        // Project y through, ignore x; the SeriesStat sees only y.
        val stat = VarianceStat().foldRegression(featureSize = 2) { _, y -> y }
        for (y in doubleArrayOf(1.0, 2.0, 3.0, 4.0, 5.0)) stat.update(feat(0.0, 0.0), y)
        val snap = stat.read(0L)
        assertEquals(5.0, snap.totalWeights, DELTA)
        assertEquals(3.0, snap.mean, DELTA)
        assertEquals(2.0, snap.variance, DELTA)
    }

    @Test
    fun `foldRegression rejects x-vector size mismatch`() {
        val stat = SumStat().foldRegression(featureSize = 3) { _, y -> y }
        try {
            stat.update(feat(1.0, 2.0), y = 0.0) // size = 2, expected 3
            error("should have thrown")
        } catch (e: IllegalArgumentException) {
            assertTrue("x.size=2" in e.message.orEmpty())
        }
    }

    @Test
    fun `chained decorators preserve featureSize and update flow`() {
        val inner = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = TreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
        )
        val stat = inner.filter { _, y -> y > 0.0 }.weightBy { _, y -> y }.throttle(every = 2)
        repeat(20) { i ->
            val y = if (i % 2 == 0) 1.0 else 2.0
            stat.update(feat(if (i % 2 == 0) -1.0 else 1.0), y)
        }
        // All y are positive so the filter passes; throttle keeps half (10); each kept
        // observation lands with weight = caller * y.
        val total = stat.read(0L).totalWeights
        assertTrue(total > 0.0, "expected updates to accumulate, got $total")
    }
}

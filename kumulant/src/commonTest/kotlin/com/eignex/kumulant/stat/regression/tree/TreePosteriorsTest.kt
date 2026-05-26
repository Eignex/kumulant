package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.abs
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
class TreePosteriorsTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    private fun snap(value: WeightedVarianceResult): TreeRegressionResult = TreeRegressionResult(TreeLeafResult(value))

    private fun split(
        threshold: Double,
        posValue: WeightedVarianceResult,
        negValue: WeightedVarianceResult,
        rootValue: WeightedVarianceResult,
    ): TreeRegressionResult = TreeRegressionResult(
        TreeSplitResult(
            split = ThresholdSplit(0, threshold),
            pos = TreeLeafResult(posValue),
            neg = TreeLeafResult(negValue),
            value = rootValue,
        ),
    )

    @Test
    fun `MeanTreePosterior returns the leaf mean and ignores rng`() {
        val s = snap(WeightedVarianceResult(10.0, 3.5, 0.25))
        val a = MeanTreePosterior.evaluate(s, feat(0.0), Random(0), 1.0)
        val b = MeanTreePosterior.evaluate(s, feat(0.0), Random(999), 0.5)
        assertEquals(3.5, a, 1e-9)
        assertEquals(a, b)
    }

    @Test
    fun `MeanTreePosterior routes through splits`() {
        val s = split(
            threshold = 0.0,
            posValue = WeightedVarianceResult(5.0, 1.0, 0.0),
            negValue = WeightedVarianceResult(5.0, -1.0, 0.0),
            rootValue = WeightedVarianceResult(10.0, 0.0, 1.0),
        )
        assertEquals(-1.0, MeanTreePosterior.evaluate(s, feat(0.5), Random(0), 1.0))
        assertEquals(1.0, MeanTreePosterior.evaluate(s, feat(-0.5), Random(0), 1.0))
    }

    @Test
    fun `ThompsonTreePosterior collapses to mean at zero exploration`() {
        val s = snap(WeightedVarianceResult(10.0, 4.0, 1.0))
        val score = ThompsonTreePosterior().evaluate(s, feat(0.0), Random(1), exploration = 0.0)
        assertEquals(4.0, score, 1e-9)
    }

    @Test
    fun `ThompsonTreePosterior produces draws centered near the mean`() {
        val s = snap(WeightedVarianceResult(100.0, 5.0, 1.0))
        val rng = Random(2)
        val draws = DoubleArray(200) {
            ThompsonTreePosterior(priorWeight = 1.0, priorVariance = 1.0)
                .evaluate(s, feat(0.0), rng, 1.0)
        }
        val mean = draws.average()
        assertTrue(abs(mean - 5.0) < 0.2, "mean=$mean expected near 5.0")
    }

    @Test
    fun `ThompsonTreePosterior falls back to prior variance on empty leaves`() {
        val s = snap(WeightedVarianceResult(0.0, 0.0, 0.0))
        // exploration = 1.0, totalWeights = 0 -> uses priorVariance = 4.0. SD is sqrt(4/1) = 2.
        val rng = Random(3)
        val draws = DoubleArray(200) {
            ThompsonTreePosterior(priorWeight = 1.0, priorVariance = 4.0)
                .evaluate(s, feat(0.0), rng, 1.0)
        }
        val sd = sampleStdDev(draws)
        assertTrue(sd > 1.0, "expected wide draws under prior, got sd=$sd")
    }

    @Test
    fun `UcbTreePosterior is deterministic and grows with exploration`() {
        val s = snap(WeightedVarianceResult(10.0, 4.0, 1.0))
        val ucb = UcbTreePosterior(priorWeight = 1.0, priorVariance = 1.0)
        val low = ucb.evaluate(s, feat(0.0), Random(0), 0.5)
        val high = ucb.evaluate(s, feat(0.0), Random(0), 2.0)
        assertTrue(high > low)
        assertTrue(low > 4.0, "should exceed mean")
    }

    private fun forestSnap(values: List<WeightedVarianceResult>): ForestRegressionResult =
        ForestRegressionResult(values.map { TreeRegressionResult(TreeLeafResult(it)) })

    @Test
    fun `MeanForestPosterior returns the weighted-average leaf mean`() {
        val f = forestSnap(
            listOf(
                WeightedVarianceResult(10.0, 1.0, 0.0),
                WeightedVarianceResult(10.0, 3.0, 0.0),
            ),
        )
        assertEquals(2.0, MeanForestPosterior.evaluate(f, feat(0.0), Random(0), 1.0), 1e-9)
    }

    @Test
    fun `ThompsonForestPosterior collapses to mean at zero exploration`() {
        val f = forestSnap(
            listOf(
                WeightedVarianceResult(10.0, 1.0, 0.0),
                WeightedVarianceResult(10.0, 3.0, 0.0),
            ),
        )
        val s = ThompsonForestPosterior().evaluate(f, feat(0.0), Random(0), 0.0)
        assertEquals(2.0, s, 1e-9)
    }

    @Test
    fun `UcbTreePosterior bonus shrinks as evidence grows`() {
        val stat = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 80)
        val rng = Random(80)
        repeat(5) { stat.update(feat(0.0), 2.0 + rng.nextDouble() - 0.5) }
        val snapEarly = stat.read(0L)
        repeat(5_000) { stat.update(feat(0.0), 2.0 + rng.nextDouble() - 0.5) }
        val snapLate = stat.read(0L)

        val posterior = UcbTreePosterior(priorWeight = 1.0, priorVariance = 1.0)
        val earlyBonus = posterior.evaluate(snapEarly, feat(0.0), Random(0), 1.0) - snapEarly.predict(feat(0.0))
        val lateBonus = posterior.evaluate(snapLate, feat(0.0), Random(0), 1.0) - snapLate.predict(feat(0.0))
        assertTrue(earlyBonus > lateBonus, "earlyBonus=$earlyBonus lateBonus=$lateBonus should shrink")
    }

    @Test
    fun `UcbForestPosterior exceeds the merged mean`() {
        val f = forestSnap(
            listOf(
                WeightedVarianceResult(10.0, 1.0, 1.0),
                WeightedVarianceResult(10.0, 1.0, 1.0),
            ),
        )
        val s = UcbForestPosterior(priorWeight = 1.0, priorVariance = 1.0)
            .evaluate(f, feat(0.0), Random(0), 1.0)
        assertTrue(s > 1.0)
    }

    private fun sampleStdDev(xs: DoubleArray): Double {
        val mean = xs.average()
        var s = 0.0
        for (x in xs) s += (x - mean) * (x - mean)
        return sqrt(s / (xs.size - 1))
    }
}

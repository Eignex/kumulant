package com.eignex.kumulant.schema

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.operation.foldRegression
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-9
private fun feat(vararg xs: Double) = DenseVector.of(xs)

class RegressionListStatsTest {

    @Test
    fun `RegressionListStats fans every update to all inner regressors`() {
        val tee = RegressionListStats<Result>(
            "tree" to DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList()),
            "sgd" to StochasticRegressionStat(featureSize = 1),
            "marginalY" to VarianceStat().foldRegression(featureSize = 1) { _, y -> y },
        )
        for (y in doubleArrayOf(1.0, 2.0, 3.0)) tee.update(feat(0.5), y)
        val snap = tee.read(0L)
        assertEquals(listOf("tree", "sgd", "marginalY"), snap.names)
        val byName = snap.toMap()
        val marginal = byName["marginalY"] as WeightedVarianceResult
        assertEquals(3.0, marginal.totalWeights, DELTA)
        assertEquals(2.0, marginal.mean, DELTA)
    }

    @Test
    fun `RegressionListStats enforces matching featureSize`() {
        try {
            RegressionListStats<Result>(
                "a" to DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList()),
                "b" to DecisionTreeRegressionStat(featureSize = 2, splitCandidates = emptyList()),
            )
            error("should have thrown")
        } catch (e: IllegalArgumentException) {
            check("featureSize" in e.message.orEmpty())
        }
    }

    @Test
    fun `tree plus marginal-y observation composes via foldRegression`() {
        val tree = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList())
        val observation = SumStat().foldRegression(featureSize = 1) { _, y -> y }
        val composite = RegressionListStats<Result>("tree" to tree, "marginalY" to observation)
        for (y in doubleArrayOf(2.0, 4.0, 6.0)) composite.update(feat(0.0), y)
        val byName = composite.read(0L).toMap()
        val sum = byName["marginalY"] as SumResult
        assertEquals(12.0, sum.sum, DELTA)
    }
}

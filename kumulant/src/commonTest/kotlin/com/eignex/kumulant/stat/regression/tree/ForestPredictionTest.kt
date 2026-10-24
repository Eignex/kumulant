package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.feat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.PI
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ForestPredictionTest {
    @Test
    fun `lognormal cost improvement remains bounded when raw moments overflow`() {
        val value = ForestPrediction(0.0, 1e6).expectedImprovementLogNormal(1.0)
        assertTrue(value.isFinite() && value in 0.49..0.5)
    }

    @Test
    fun `lognormal expected improvement agrees with integrated log response moments`() {
        val prediction = ForestPrediction(0.0, 1.0)
        assertEquals(0.2384217081, prediction.expectedImprovementLogNormal(1.0), 1e-6)
        assertEquals(0.8871429788, prediction.expectedImprovementLogNormal(1.0, maximize = true), 1e-6)
    }

    @Test
    fun `lognormal deterministic improvement uses original units and margin`() {
        val prediction = ForestPrediction(kotlin.math.ln(2.0), 0.0)
        assertEquals(1.0, prediction.expectedImprovementLogNormal(3.0))
        assertEquals(0.5, prediction.expectedImprovementLogNormal(3.0, margin = 0.5))
        assertEquals(1.0, prediction.expectedImprovementLogNormal(1.0, maximize = true))
        assertEquals(0.0, prediction.expectedImprovementLogNormal(1.0, margin = 2.0))
    }

    @Test
    fun `forest distribution weights model predictions equally`() {
        val forest = ForestRegressionResult(
            listOf(
                TreeRegressionResult(TreeLeafResult(WeightedVarianceResult(1.0, 1.0, 100.0))),
                TreeRegressionResult(TreeLeafResult(WeightedVarianceResult(100.0, 3.0, 100.0))),
            ),
        )
        assertEquals(ForestPrediction(2.0, 1.0), forest.predictDistribution(feat(0.0)))
    }

    @Test
    fun `zero variance expected improvement is deterministic improvement`() {
        assertEquals(1.0, ForestPrediction(3.0, 0.0).expectedImprovement(2.0))
        assertEquals(0.0, ForestPrediction(1.0, 0.0).expectedImprovement(2.0))
        assertEquals(1.0, ForestPrediction(1.0, 0.0).expectedImprovement(2.0, maximize = false))
    }

    @Test
    fun `uncertainty gives positive improvement at the incumbent mean`() {
        assertEquals(2.0 / sqrt(2.0 * PI), ForestPrediction(2.0, 4.0).expectedImprovement(2.0), 1e-12)
    }

    @Test
    fun `Gaussian expected improvement agrees with a known value`() {
        assertEquals(0.0833154706, ForestPrediction(0.0, 1.0).expectedImprovement(1.0), 1e-7)
    }

    @Test
    fun `minimization is equivalent to sign flipped maximization`() {
        val minimizing = ForestPrediction(1.0, 0.5).expectedImprovement(2.0, maximize = false)
        val maximizing = ForestPrediction(-1.0, 0.5).expectedImprovement(-2.0)
        assertEquals(minimizing, maximizing, 1e-12)
    }

    @Test
    fun `improvement margin reduces acquisition value`() {
        val prediction = ForestPrediction(1.0, 1.0)
        assertTrue(prediction.expectedImprovement(1.0, margin = 0.5) < prediction.expectedImprovement(1.0))
    }
}

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.feat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals

class ForestPredictionTest {
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
    fun `forest distribution skips trees whose routed leaf is empty`() {
        val forest = ForestRegressionResult(
            listOf(
                TreeRegressionResult(TreeLeafResult(WeightedVarianceResult(1.0, 10.0, 0.0))),
                TreeRegressionResult(TreeLeafResult(WeightedVarianceResult(0.0, 0.0, 0.0))),
            ),
        )
        assertEquals(ForestPrediction(10.0, 0.0), forest.predictDistribution(feat(0.0)))
    }

    @Test
    fun `forest distribution over only empty leaves is zero`() {
        val forest = ForestRegressionResult(
            listOf(TreeRegressionResult(TreeLeafResult(WeightedVarianceResult(0.0, 0.0, 0.0)))),
        )
        assertEquals(ForestPrediction(0.0, 0.0), forest.predictDistribution(feat(0.0)))
    }
}

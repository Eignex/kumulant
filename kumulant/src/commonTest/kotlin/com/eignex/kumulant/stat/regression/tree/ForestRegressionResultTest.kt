package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ForestRegressionResultTest {

    private fun leaf(w: Double, mean: Double, variance: Double) =
        TreeRegressionResult(TreeLeafResult(WeightedVarianceResult(w, mean, variance)))

    @Test
    fun `findLeafMerged returns Chan-pooled mean and variance across trees`() {
        // Three single-leaf trees with disjoint (w, mean, var). Chan's parallel formula:
        //   w  = w1 + w2; mean = (w1*m1 + w2*m2)/w; sst = v1*w1 + v2*w2 + (m2-m1)^2 * w1*w2/w
        val forest = ForestRegressionResult(
            trees = listOf(
                leaf(2.0, 1.0, 0.5),
                leaf(3.0, 5.0, 1.0),
            ),
        )
        val r = forest.findLeafMerged(DenseVector.of(doubleArrayOf(0.0)))
        val w = 5.0
        val expectedMean = (2.0 * 1.0 + 3.0 * 5.0) / w
        val delta = 5.0 - 1.0
        val sst = 0.5 * 2.0 + 1.0 * 3.0 + delta * delta * (2.0 * 3.0 / w)
        assertEquals(w, r.totalWeights, 1e-12)
        assertEquals(expectedMean, r.mean, 1e-12)
        assertEquals(sst / w, r.variance, 1e-12)
    }

    @Test
    fun `findLeafMerged skips zero-weight leaves`() {
        val forest = ForestRegressionResult(
            trees = listOf(
                leaf(0.0, 99.0, 99.0), // ignored
                leaf(4.0, 2.0, 1.0),
            ),
        )
        val r = forest.findLeafMerged(DenseVector.of(doubleArrayOf(0.0)))
        assertEquals(4.0, r.totalWeights, 1e-12)
        assertEquals(2.0, r.mean, 1e-12)
        assertEquals(1.0, r.variance, 1e-12)
    }

    @Test
    fun `predict equals merged mean`() {
        val forest = ForestRegressionResult(
            trees = listOf(leaf(1.0, 3.0, 0.0), leaf(1.0, 5.0, 0.0)),
        )
        assertEquals(4.0, forest.predict(DenseVector.of(doubleArrayOf(0.0))), 1e-12)
    }

    @Test
    fun `totalWeights sums per-tree totals`() {
        val forest = ForestRegressionResult(
            trees = listOf(leaf(2.0, 1.0, 0.0), leaf(3.0, 1.0, 0.0), leaf(5.0, 1.0, 0.0)),
        )
        assertEquals(10.0, forest.totalWeights, 1e-12)
    }

    @Test
    fun `init rejects empty tree list`() {
        assertFailsWith<IllegalArgumentException> { ForestRegressionResult(emptyList()) }
    }

    @Test
    fun `all-empty leaves merge to a zero-weight result`() {
        val forest = ForestRegressionResult(
            trees = listOf(leaf(0.0, 0.0, 0.0), leaf(0.0, 0.0, 0.0)),
        )
        val r = forest.findLeafMerged(DenseVector.of(doubleArrayOf(0.0)))
        assertEquals(0.0, r.totalWeights, 1e-12)
        assertEquals(0.0, r.variance, 1e-12)
    }
}

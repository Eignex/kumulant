package com.eignex.kumulant.stat.regression

import com.eignex.koblas.SparseVector
import com.eignex.kumulant.assertAllocatesAtMost
import com.eignex.kumulant.bandit.contextual.KnnContextualBandit
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import kotlin.test.Test

class OwnedStorageAllocationTest {

    @Test
    fun `reads allocate only their retained snapshot storage`() {
        val x = DoubleArray(32) { it.toDouble() }
        val stochastic = StochasticRegressionStat(32).also { it.update(x, 1.0) }
        val diagonal = DiagonalRegressionStat(32).also { it.update(x, 1.0) }
        val bayesian = BayesianRegressionStat(32).also { it.update(x, 1.0) }
        val naiveBayes = GaussianNaiveBayesStat(32, 4).also { it.update(x, 0.0) }

        assertAllocatesAtMost(400.0, "stochastic read") { stochastic.read() }
        assertAllocatesAtMost(700.0, "diagonal read") { diagonal.read() }
        assertAllocatesAtMost(17_000.0, "Bayesian read") { bayesian.read() }
        assertAllocatesAtMost(2_500.0, "Gaussian NB read") { naiveBayes.read() }
    }

    @Test
    fun `regression merges avoid snapshot materialisation`() {
        val x = DoubleArray(32) { it.toDouble() }
        val stochastic = StochasticRegressionStat(32).also { it.update(x, 1.0) }
        val diagonal = DiagonalRegressionStat(32).also { it.update(x, 1.0) }
        val stochasticValue = stochastic.read()
        val diagonalValue = diagonal.read()

        assertAllocatesAtMost(0.0, "stochastic merge") { stochastic.merge(stochasticValue) }
        assertAllocatesAtMost(0.0, "diagonal merge") { diagonal.merge(diagonalValue) }
    }

    @Test
    fun `sparse updates retain nnz sized storage without feature sized scratch`() {
        for (featureSize in intArrayOf(32, 128, 512)) {
            for (density in intArrayOf(1, 10, 100)) {
                val nnz = (featureSize * density / 100).coerceAtLeast(1)
                val sparse = SparseVector.of(
                    featureSize,
                    IntArray(nnz) { it * featureSize / nnz },
                    DoubleArray(nnz) { it.toDouble() },
                )
                val bandit = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 1, exploration = 0.0)
                val retainedBytes = nnz * 12.0 + 128.0

                assertAllocatesAtMost(
                    retainedBytes + 256.0,
                    "$featureSize features at $density% density with $retainedBytes B of retained storage",
                ) { bandit.update(0, sparse, 1.0) }
            }
        }
    }
}

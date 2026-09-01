package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64SparseVector
import com.eignex.kumulant.bandit.contextual.KnnContextualBandit
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class OwnedStorageAllocationTest {

    private val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean

    private fun interface Body {
        fun run()
    }

    private fun bytesPerCall(body: Body): Double {
        repeat(1_000) { body.run() }
        val id = Thread.currentThread().threadId()
        var best = Double.MAX_VALUE
        repeat(5) {
            val before = bean.getThreadAllocatedBytes(id)
            repeat(2_000) { body.run() }
            val after = bean.getThreadAllocatedBytes(id)
            best = minOf(best, (after - before).toDouble() / 2_000)
        }
        return best
    }

    @Test
    fun `reads allocate only their retained snapshot storage`() {
        val x = DoubleArray(32) { it.toDouble() }
        val stochastic = StochasticRegressionStat(32).also { it.update(x, 1.0) }
        val diagonal = DiagonalRegressionStat(32).also { it.update(x, 1.0) }
        val bayesian = BayesianRegressionStat(32).also { it.update(x, 1.0) }
        val naiveBayes = GaussianNaiveBayesStat(32, 4).also { it.update(x, 0.0) }

        val stochasticBytes = bytesPerCall { stochastic.read() }
        val diagonalBytes = bytesPerCall { diagonal.read() }
        val bayesianBytes = bytesPerCall { bayesian.read() }
        val naiveBayesBytes = bytesPerCall { naiveBayes.read() }

        assertTrue(stochasticBytes < 400.0, "stochastic read allocated $stochasticBytes B/call")
        assertTrue(diagonalBytes < 700.0, "diagonal read allocated $diagonalBytes B/call")
        assertTrue(bayesianBytes < 17_000.0, "Bayesian read allocated $bayesianBytes B/call")
        assertTrue(naiveBayesBytes < 2_500.0, "Gaussian NB read allocated $naiveBayesBytes B/call")
    }

    @Test
    fun `regression merges avoid snapshot materialisation`() {
        val x = DoubleArray(32) { it.toDouble() }
        val stochastic = StochasticRegressionStat(32).also { it.update(x, 1.0) }
        val diagonal = DiagonalRegressionStat(32).also { it.update(x, 1.0) }
        val stochasticValue = stochastic.read()
        val diagonalValue = diagonal.read()

        val stochasticBytes = bytesPerCall { stochastic.merge(stochasticValue) }
        val diagonalBytes = bytesPerCall { diagonal.merge(diagonalValue) }

        assertTrue(stochasticBytes < 64.0, "stochastic merge allocated $stochasticBytes B/call")
        assertTrue(diagonalBytes < 64.0, "diagonal merge allocated $diagonalBytes B/call")
    }

    @Test
    fun `sparse updates retain nnz sized storage without feature sized scratch`() {
        for (featureSize in intArrayOf(32, 128, 512)) {
            for (density in intArrayOf(1, 10, 100)) {
                val nnz = (featureSize * density / 100).coerceAtLeast(1)
                val sparse = F64SparseVector.of(
                    featureSize,
                    IntArray(nnz) { it * featureSize / nnz },
                    DoubleArray(nnz) { it.toDouble() },
                )
                val bandit = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 1, exploration = 0.0)
                val allocatedBytes = bytesPerCall { bandit.update(0, sparse, 1.0) }
                val retainedBytes = nnz * 12.0 + 128.0

                assertTrue(
                    allocatedBytes < retainedBytes + 256.0,
                    "$featureSize features at $density% density allocated $allocatedBytes B/call; " +
                        "retained storage is $retainedBytes B",
                )
            }
        }
    }
}

package com.eignex.kumulant.bench

import com.eignex.koblas.core.F64SparseVector
import com.eignex.kumulant.bandit.contextual.KnnContextualBandit
import com.eignex.kumulant.stat.regression.GaussianNaiveBayesStat
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class OwnedStorageBenchmark {
    @Param("32", "128", "512")
    var featureSize: Int = 32

    @Param("1", "10", "100")
    var densityPercent: Int = 1

    private lateinit var stochastic: StochasticRegressionStat
    private lateinit var diagonal: DiagonalRegressionStat
    private lateinit var bayesian: BayesianRegressionStat
    private lateinit var naiveBayes: GaussianNaiveBayesStat
    private lateinit var sparse: F64SparseVector
    private lateinit var knn: KnnContextualBandit

    @Setup
    fun setup() {
        val x = DoubleArray(featureSize) { (it % 7 - 3).toDouble() }
        stochastic = StochasticRegressionStat(featureSize).also { it.update(x, 1.0) }
        diagonal = DiagonalRegressionStat(featureSize).also { it.update(x, 1.0) }
        bayesian = BayesianRegressionStat(featureSize).also { it.update(x, 1.0) }
        naiveBayes = GaussianNaiveBayesStat(featureSize, 4).also { it.update(x, 0.0) }
        val nnz = (featureSize * densityPercent / 100).coerceAtLeast(1)
        sparse = F64SparseVector.of(
            featureSize,
            IntArray(nnz) { it * featureSize / nnz },
            DoubleArray(nnz) { it.toDouble() },
        )
        knn = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 1, exploration = 0.0)
    }

    @Benchmark fun stochasticRead() = stochastic.read()
    @Benchmark fun diagonalRead() = diagonal.read()
    @Benchmark fun bayesianRead() = bayesian.read()
    @Benchmark fun gaussianNaiveBayesRead() = naiveBayes.read()
    @Benchmark fun sparseKnnUpdate(): Unit = knn.update(0, sparse, 1.0)
}

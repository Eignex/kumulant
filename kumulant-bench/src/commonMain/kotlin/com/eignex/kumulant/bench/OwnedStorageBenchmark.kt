package com.eignex.kumulant.bench

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

    private lateinit var stochastic: StochasticRegressionStat
    private lateinit var diagonal: DiagonalRegressionStat
    private lateinit var bayesian: BayesianRegressionStat
    private lateinit var naiveBayes: GaussianNaiveBayesStat

    @Setup
    fun setup() {
        val x = DoubleArray(featureSize) { (it % 7 - 3).toDouble() }
        stochastic = StochasticRegressionStat(featureSize).also { it.update(x, 1.0) }
        diagonal = DiagonalRegressionStat(featureSize).also { it.update(x, 1.0) }
        bayesian = BayesianRegressionStat(featureSize).also { it.update(x, 1.0) }
        naiveBayes = GaussianNaiveBayesStat(featureSize, 4).also { it.update(x, 0.0) }
    }

    @Benchmark fun stochasticRead() = stochastic.read()
    @Benchmark fun diagonalRead() = diagonal.read()
    @Benchmark fun bayesianRead() = bayesian.read()
    @Benchmark fun gaussianNaiveBayesRead() = naiveBayes.read()
}

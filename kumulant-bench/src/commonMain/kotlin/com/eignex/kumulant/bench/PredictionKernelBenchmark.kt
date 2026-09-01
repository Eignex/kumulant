package com.eignex.kumulant.bench

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.Workspace
import com.eignex.kumulant.stat.regression.SoftmaxRegressionResult
import com.eignex.kumulant.stat.regression.GaussianNaiveBayesStat
import com.eignex.kumulant.bandit.contextual.KnnContextualBandit
import com.eignex.kumulant.stat.regression.glm.CovarianceRegressionResult
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.Link
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionResult
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State
import kotlin.random.Random

/** Read-only prediction kernels over dense and sparse feature vectors. */
@State(Scope.Benchmark)
open class PredictionKernelBenchmark {
    @Param("8", "32", "128", "512")
    var featureSize: Int = 8

    @Param("1", "10", "100")
    var densityPercent: Int = 1

    private lateinit var x: F64VectorLike
    private lateinit var linear: StochasticRegressionResult
    private lateinit var softmax: SoftmaxRegressionResult
    private lateinit var posterior: CovarianceRegressionResult
    private lateinit var gaussianNaiveBayes: com.eignex.kumulant.stat.regression.GaussianNaiveBayesResult
    private lateinit var knn: KnnContextualBandit
    private lateinit var workspace: Workspace
    private lateinit var probabilities: DoubleArray

    @Setup
    fun setup() {
        val values = DoubleArray(featureSize) { i -> (i % 11 - 5) * 0.125 }
        val count = maxOf(1, featureSize * densityPercent / 100)
        x = if (densityPercent == 100) F64DenseVector.of(values) else F64SparseVector.of(
            featureSize,
            IntArray(count) { it * featureSize / count },
            DoubleArray(count) { values[it * featureSize / count] },
        )
        val weights = F64DenseVector.of(DoubleArray(featureSize) { i -> (i % 7 - 3) * 0.2 })
        linear = StochasticRegressionResult(weights, 0.25, 0.0, 0L, Link.Identity)
        softmax = SoftmaxRegressionResult(
            featureSize,
            4,
            F64DenseMatrix.of(Array(4) { k -> DoubleArray(featureSize) { i -> (k - i % 5) * 0.1 } }),
            F64DenseVector.of(doubleArrayOf(-0.2, 0.0, 0.1, 0.3)),
            0.0,
            0L,
            0.0,
        )
        val identity = F64DenseMatrix.diagonal(featureSize, 1.0)
        posterior = CovarianceRegressionResult(weights, 0.25, 1.0, 0.0, 0L, identity, identity)
        val naiveBayes = GaussianNaiveBayesStat(featureSize, 4)
        repeat(16) { sample ->
            naiveBayes.update(F64DenseVector.of(DoubleArray(featureSize) { i -> (sample - i % 7) * 0.1 }), (sample % 4).toDouble())
        }
        gaussianNaiveBayes = naiveBayes.read()
        knn = KnnContextualBandit(nbrArms = 4, k = 8, exploration = 0.0)
        repeat(4) { arm ->
            repeat(32) { sample -> knn.update(arm, x, (sample - arm).toDouble()) }
        }
        workspace = Workspace().apply {
            reserve(featureSize, count = 3)
            reserve(4, count = 1)
            reserve(24, count = 1)
        }
        probabilities = DoubleArray(4)
    }

    @Benchmark
    fun linearPredict(): Double = linear.predict(x)

    @Benchmark
    fun softmaxProbabilities(): DoubleArray = softmax.probabilities(x)

    @Benchmark
    fun softmaxPredict(): Int = softmax.predict(x)

    @Benchmark
    fun softmaxProbabilitiesInto(): DoubleArray = probabilities.also { softmax.probabilitiesInto(x, it) }

    @Benchmark
    fun softmaxPredictWorkspace(): Int = softmax.predict(x, workspace)

    @Benchmark
    fun gaussianNaiveBayesProbabilities(): DoubleArray = gaussianNaiveBayes.probabilities(x)

    @Benchmark
    fun gaussianNaiveBayesProbabilitiesInto(): DoubleArray = probabilities.also { gaussianNaiveBayes.probabilitiesInto(x, it) }

    @Benchmark
    fun knnChoose(): Int = knn.choose(x)

    @Benchmark
    fun knnChooseWorkspace(): Int = knn.choose(x, workspace)

    @Benchmark
    fun multivariateSample(): F64VectorLike = MultivariateGaussian.sample(posterior, Random(1234), 1.0)

    @Benchmark
    fun multivariateEvaluate(): Double = MultivariateGaussian.evaluate(posterior, x, Random(1234), 1.0)

    @Benchmark
    fun multivariateEvaluateWorkspace(): Double =
        MultivariateGaussian.evaluate(posterior, x, Random(1234), workspace, 1.0)

    @Benchmark
    fun bayesianPrior(): CovarianceRegressionResult = BayesianRegressionStat(
        featureSize = featureSize,
        priorMean = F64DenseVector.of(DoubleArray(featureSize) { it * 0.01 }),
    ).read()

    @Benchmark
    fun bayesianMerge(): CovarianceRegressionResult {
        val left = BayesianRegressionStat(featureSize)
        val right = BayesianRegressionStat(featureSize)
        left.update(x, 1.0)
        right.update(x, -0.5)
        left.merge(right.read())
        return left.read()
    }
}

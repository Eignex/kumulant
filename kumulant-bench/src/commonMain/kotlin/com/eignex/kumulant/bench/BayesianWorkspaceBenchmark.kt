package com.eignex.kumulant.bench

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseVector
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.PrecisionRegressionResult
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

/** Repeated Bayesian update and merge kernels with setup excluded from measurements. */
@State(Scope.Benchmark)
open class BayesianWorkspaceBenchmark {
    @Param("8", "32", "128", "512")
    var featureSize: Int = 8

    private lateinit var x: F64DenseVector
    private lateinit var workspace: Workspace
    private lateinit var stat: BayesianRegressionStat
    private lateinit var mergeValue: PrecisionRegressionResult

    @Setup
    fun setup() {
        x = F64DenseVector.of(DoubleArray(featureSize) { (it % 7 - 3) * 0.125 })
        workspace = Workspace().apply { reserve(featureSize, count = 3) }
        stat = BayesianRegressionStat(featureSize)
        val other = BayesianRegressionStat(featureSize)
        other.update(x, -0.5)
        mergeValue = other.read()
    }

    @Benchmark
    fun update(): Unit = stat.update(x, 1.0)

    @Benchmark
    fun updateWorkspace(): Unit = stat.update(x, 1.0, workspace = workspace)

    @Benchmark
    fun merge(): Unit = stat.merge(mergeValue)

    @Benchmark
    fun mergeWorkspace(): Unit = stat.merge(mergeValue, workspace)
}

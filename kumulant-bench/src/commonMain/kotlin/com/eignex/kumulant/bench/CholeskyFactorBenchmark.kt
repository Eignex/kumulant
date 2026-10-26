package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.kumulant.math.CholeskyFactor
import com.eignex.kumulant.math.factorCholesky
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CholeskyFactorBenchmark {
    @Param(
        "precision:8", "precision:32", "precision:128", "precision:512",
        "rbf:16", "rbf:32", "rbf:64", "rbf:128", "rbf:256", "rbf:512", "rbf:1024",
        "matern:64", "matern:256", "matern:1024",
    )
    lateinit var matrixCase: String

    private lateinit var matrix: DenseMatrix
    private val workspace = Workspace()

    @Setup
    fun setup() {
        reportCholeskyBackend()
        matrix = CholeskyFixtures.matrix(matrixCase)
    }

    @Benchmark
    fun factor(): CholeskyFactor = matrix.factorCholesky()

    @Benchmark
    fun factorWithWorkspace(): CholeskyFactor = matrix.factorCholesky(workspace)
}

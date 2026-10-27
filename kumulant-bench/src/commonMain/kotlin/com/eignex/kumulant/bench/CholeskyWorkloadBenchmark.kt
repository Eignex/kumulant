package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.kumulant.math.factorCholesky
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Blackhole
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CholeskyWorkloadBenchmark {
    @Param("32", "128", "512")
    var historySize: Int = 32

    private lateinit var points: DoubleArray
    private lateinit var covariance: DenseMatrix
    private lateinit var fittingMatrices: List<DenseMatrix>
    private lateinit var right: DoubleArray
    private lateinit var out: DoubleArray
    private val workspace = Workspace()

    @Setup
    fun setup() {
        reportCholeskyBackend()
        points = CholeskyFixtures.points(historySize)
        covariance = CholeskyFixtures.covariance(points)
        right = CholeskyFixtures.right(historySize).values
        out = DoubleArray(historySize)
        // Stress orders keep only one covariance live; 25 order-2048 candidates alone exceed the 512 MiB harness heap.
        fittingMatrices = if (historySize > CholeskyFixtures.FITTING_ROUND_MAX_ORDER) emptyList() else {
            List(CholeskyFixtures.FITTING_ROUND_SIZE) { candidate ->
                CholeskyFixtures.covariance(points, lengthScale = 0.05 + candidate * 0.01)
            }
        }
    }

    @Benchmark
    fun covarianceConstruction(): DenseMatrix = CholeskyFixtures.covariance(points)

    @Benchmark
    fun factorSolveLogDeterminant(blackhole: Blackhole): Unit = evaluate(covariance, blackhole)

    @Benchmark
    fun factorSolveLogDeterminantWithWorkspace(blackhole: Blackhole) {
        val factor = covariance.factorCholesky(workspace)
        blackhole.consume(factor.solveInto(right, out, workspace))
        blackhole.consume(factor.logDeterminant())
    }

    @Benchmark
    fun covarianceFactorSolveLogDeterminant(blackhole: Blackhole): Unit =
        evaluate(CholeskyFixtures.covariance(points), blackhole)

    @Benchmark
    fun fittingRound(blackhole: Blackhole) {
        requireFittingRound()
        for (matrix in fittingMatrices) evaluate(matrix, blackhole)
    }

    @Benchmark
    fun fittingRoundWithWorkspace(blackhole: Blackhole) {
        requireFittingRound()
        for (matrix in fittingMatrices) {
            val factor = matrix.factorCholesky(workspace)
            blackhole.consume(factor.solveInto(right, out, workspace))
            blackhole.consume(factor.logDeterminant())
        }
    }

    private fun requireFittingRound() = check(fittingMatrices.isNotEmpty()) {
        "Fitting rounds hold ${CholeskyFixtures.FITTING_ROUND_SIZE} covariances; history sizes above " +
            "${CholeskyFixtures.FITTING_ROUND_MAX_ORDER} measure single factor/solve/log-determinant evaluations"
    }

    private fun evaluate(matrix: DenseMatrix, blackhole: Blackhole) {
        val factor = matrix.factorCholesky()
        blackhole.consume(factor.solve(right))
        blackhole.consume(factor.logDeterminant())
    }
}

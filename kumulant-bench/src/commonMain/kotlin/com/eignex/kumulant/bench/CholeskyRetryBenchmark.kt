package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.kumulant.math.CholeskyFactor
import com.eignex.kumulant.math.NotPositiveDefinite
import com.eignex.kumulant.math.factorCholesky
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Blackhole
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CholeskyRetryBenchmark {
    @Param("success", "retry", "exhaustion")
    lateinit var scenario: String

    private lateinit var matrix: DenseMatrix
    private val workspace = Workspace()

    @Setup
    fun setup() {
        matrix = retryMatrix(scenario)
    }

    @Benchmark
    fun callerRetry(blackhole: Blackhole) {
        val result = retryFactor(matrix, workspace)
        blackhole.consume(result.factor)
        blackhole.consume(result.attempts)
        blackhole.consume(result.shift)
    }
}

internal data class CholeskyRetryResult(val factor: CholeskyFactor?, val attempts: Int, val shift: Double)

private val RETRY_SHIFTS = doubleArrayOf(0.0, 1e-12, 1e-10, 1e-8)

internal fun retryMatrix(scenario: String): DenseMatrix = DenseMatrix.diagonal(65, 1.0).apply {
    this[64, 64] = when (scenario) {
        "success" -> 1.0
        "retry" -> -1e-10
        "exhaustion" -> -1.0
        else -> error("Unknown retry scenario: $scenario")
    }
}

internal fun retryFactor(matrix: DenseMatrix, workspace: Workspace): CholeskyRetryResult {
    for ((attempt, shift) in RETRY_SHIFTS.withIndex()) {
        val shifted = DenseMatrix.wrap(matrix.rows, matrix.cols, matrix.values.copyOf())
        for (i in 0 until matrix.rows) shifted[i, i] += shift
        try {
            return CholeskyRetryResult(shifted.factorCholesky(workspace), attempt + 1, shift)
        } catch (_: NotPositiveDefinite) {
            if (attempt == RETRY_SHIFTS.lastIndex) return CholeskyRetryResult(null, RETRY_SHIFTS.size, shift)
        }
    }
    error("No retry attempts configured")
}

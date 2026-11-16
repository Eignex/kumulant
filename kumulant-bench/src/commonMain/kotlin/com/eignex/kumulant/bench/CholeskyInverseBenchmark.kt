package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.koblas
import com.eignex.kumulant.math.factorCholesky
import com.eignex.kumulant.stat.regression.glm.PrecisionRegressionResult
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CholeskyInverseBenchmark {
    @Param("precision:32", "precision:128", "precision:512")
    lateinit var matrixCase: String

    private lateinit var posterior: PrecisionRegressionResult
    private lateinit var out: DenseMatrix
    private lateinit var work: DoubleArray

    @Setup
    fun setup() {
        reportCholeskyBackend()
        val lower = CholeskyFixtures.matrix(matrixCase).factorCholesky().lowerFactor()
        posterior = PrecisionRegressionResult(DenseVector.zero(lower.rows), 0.0, 1.0, 1.0, 1L, lower)
        out = DenseMatrix.zero(lower.rows, lower.cols)
        work = DoubleArray(lower.rows)
    }

    @Benchmark
    fun inverseInto(): DenseMatrix = posterior.covarianceInto(out)

    @Benchmark
    fun columnWiseInverseInto(): DenseMatrix {
        val n = out.rows
        val lower = posterior.precisionL.values
        val destination = out.values
        val kernels = koblas
        for (j in 0 until n) {
            work.fill(0.0, j, n)
            work[j] = 1.0
            for (column in j until n) {
                val base = column + column * n
                val coefficient = work[column] / lower[base]
                work[column] = coefficient
                if (coefficient != 0.0) kernels.axpy(work, column + 1, -coefficient, lower, base + 1, n - column - 1)
            }
            for (i in n - 1 downTo j) {
                val base = i + i * n
                work[i] = (work[i] - kernels.dot(lower, base + 1, work, i + 1, n - i - 1)) / lower[base]
            }
            for (i in j until n) {
                destination[i + j * n] = work[i]
                destination[j + i * n] = work[i]
            }
        }
        return out
    }
}

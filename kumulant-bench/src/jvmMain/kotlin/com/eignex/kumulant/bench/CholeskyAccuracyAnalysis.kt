package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.koblas
import com.eignex.kumulant.math.factorCholesky
import java.util.Locale
import kotlin.math.abs
import kotlin.math.ln

internal fun analyzeCholeskyAccuracy() {
    println("Cholesky accuracy: seed=${CholeskyFixtures.SEED}, backend=$koblas")
    println("noiseVariance=${CholeskyFixtures.NOISE_VARIANCE}, jitter=${CholeskyFixtures.JITTER}")
    println("fixture,size,scale,relative_reconstruction,solve_backward_error")
    for (kind in listOf("precision", "rbf", "matern")) {
        for (size in listOf(63, 64, 65, 127, 128, 129)) {
            for (scale in listOf(1e-100, 1.0, 1e100)) {
                val matrix = CholeskyFixtures.matrix("$kind:$size")
                for (i in matrix.values.indices) matrix.values[i] *= scale
                val factor = matrix.factorCholesky()
                val right = CholeskyFixtures.right(size, 16)
                val solution = factor.solve(right)
                val reconstruction = relativeReconstructionError(matrix, factor.lowerFactor())
                val backward = solveBackwardError(matrix, right, solution)
                check(reconstruction < 1e-13 && backward < 1e-13) { "$kind:$size at $scale: reconstruction=$reconstruction, backward=$backward" }
                println("$kind,$size,$scale,${reconstruction.scientific()},${backward.scientific()}")
            }
        }
    }
    println("diagonal,size,scale,absolute_log_determinant_error")
    for (scale in listOf(1e-100, 1.0, 1e100)) {
        val matrix = DenseMatrix.diagonal(65, scale)
        val error = abs(matrix.factorCholesky().logDeterminant() - 65 * ln(scale))
        check(error <= 1e-10) { "Diagonal log determinant error=$error" }
        println("diagonal,65,$scale,${error.scientific()}")
    }
}

private fun relativeReconstructionError(matrix: DenseMatrix, lower: DenseMatrix): Double {
    var error = 0.0
    var scale = 0.0
    for (j in 0 until matrix.cols) {
        for (i in j until matrix.rows) {
            var actual = 0.0
            for (k in 0..j) actual += lower[i, k] * lower[j, k]
            error = maxOf(error, abs(actual - matrix[i, j]))
            scale = maxOf(scale, abs(matrix[i, j]))
        }
    }
    return error / scale
}

private fun solveBackwardError(matrix: DenseMatrix, right: DenseMatrix, solution: DenseMatrix): Double {
    var matrixNorm = 0.0
    for (i in 0 until matrix.rows) {
        var rowSum = 0.0
        for (j in 0 until matrix.cols) rowSum += abs(matrix[i, j])
        matrixNorm = maxOf(matrixNorm, rowSum)
    }
    var error = 0.0
    for (column in 0 until right.cols) {
        var residual = 0.0
        var solutionNorm = 0.0
        var rightNorm = 0.0
        for (i in 0 until matrix.rows) {
            var actual = 0.0
            for (j in 0 until matrix.cols) actual += matrix[i, j] * solution[j, column]
            residual = maxOf(residual, abs(actual - right[i, column]))
            solutionNorm = maxOf(solutionNorm, abs(solution[i, column]))
            rightNorm = maxOf(rightNorm, abs(right[i, column]))
        }
        error = maxOf(error, residual / (matrixNorm * solutionNorm + rightNorm))
    }
    return error
}

private fun Double.scientific(): String = String.format(Locale.ROOT, "%.6e", this)

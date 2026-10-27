package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.koblas.koblas
import com.eignex.kumulant.math.CholeskyFactor
import com.eignex.kumulant.math.factorCholesky
import kotlin.math.abs
import kotlin.math.ln
import kotlin.math.sqrt

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
                println("$kind,$size,$scale,$reconstruction,$backward")
            }
        }
    }
    println("diagonal,size,scale,absolute_log_determinant_error")
    for (scale in listOf(1e-100, 1.0, 1e100)) {
        val matrix = DenseMatrix.diagonal(65, scale)
        val error = abs(matrix.factorCholesky().logDeterminant() - 65 * ln(scale))
        check(error <= 1e-10) { "Diagonal log determinant error=$error" }
        println("diagonal,65,$scale,$error")
    }
    analyzeUpdates()
    analyzeExtensions()
    println("retry,scenario,attempts,diagonal_shift,succeeded")
    for (scenario in listOf("success", "retry", "exhaustion")) {
        val result = retryFactor(retryMatrix(scenario), Workspace())
        check((result.factor != null) == (scenario != "exhaustion"))
        println("retry,$scenario,${result.attempts},${result.shift},${result.factor != null}")
    }
}

private fun analyzeUpdates() {
    println("update,size,scale,reconstruction,backward_error,relative_solution_error,absolute_logdet_error")
    val workspace = Workspace()
    for (size in listOf(8, 32, 128, 512)) {
        for (scale in listOf(1e-100, 1.0, 1e100)) {
            val matrix = CholeskyFixtures.precision(size)
            for (i in matrix.values.indices) matrix.values[i] *= scale
            var factor = matrix.factorCholesky(workspace)
            val updates = CholeskyFixtures.right(size, 8)
            for (column in 0 until updates.cols) {
                val update = DoubleArray(size) { updates[it, column] * sqrt(scale) }
                factor = factor.rankUpdated(update, workspace)
                CholeskyFixtures.addOuterProduct(matrix, update)
            }
            val errors = compareFactors(matrix, factor)
            check(errors[0] < 1e-13 && errors[1] < 1e-13 && errors[2] < 1e-10 && errors[3] < 1e-9) {
                "rank updates size=$size scale=$scale errors=${errors.toList()}"
            }
            println("update,$size,$scale,${errors.joinToString(",")}")
        }
    }
}

private fun analyzeExtensions() {
    println("extension,kernel,size,appended,scale,reconstruction,backward_error,relative_solution_error,absolute_logdet_error,forward_tolerance")
    val workspace = Workspace()
    for (kind in listOf("rbf", "matern")) {
        for (size in listOf(63, 64, 65, 127, 128, 129)) {
            for (added in listOf(1, 4, 16)) {
                for (scale in listOf(1e-100, 1.0, 1e100)) {
                    val fixture = CholeskyFixtures.extension(size, added, kind, scale)
                    val factor = fixture.base.factorCholesky(workspace).extended(fixture.cross, fixture.diagonal, workspace)
                    val errors = compareFactors(fixture.full, factor)
                    val tolerance = 64 * 2.220446049250313e-16 * (size + added) / CholeskyFixtures.NOISE_VARIANCE
                    check(errors[0] < 1e-13 && errors[1] < 1e-13 && errors[2] < tolerance && errors[3] < tolerance) {
                        "extension $kind:$size+$added scale=$scale errors=${errors.toList()} tolerance=$tolerance"
                    }
                    println("extension,$kind,$size,$added,$scale,${errors.joinToString(",")},${tolerance}")
                }
            }
        }
    }
}

internal fun analyzeCholeskyStress(order: Int) {
    require(order > STRESS_APPENDED) { "Stress order must exceed $STRESS_APPENDED" }
    val tolerance = 64 * 2.220446049250313e-16 * order / CholeskyFixtures.NOISE_VARIANCE
    println("Cholesky stress: seed=${CholeskyFixtures.SEED}, order=$order, backend=$koblas")
    println("noiseVariance=${CholeskyFixtures.NOISE_VARIANCE}, jitter=${CholeskyFixtures.JITTER}, matrixBytes=${8L * order * order}")
    println("stress_factor,kernel,order,relative_reconstruction,solve_backward_error,into_backward_error,aliased_backward_error,workspace_factor_difference")
    val workspace = Workspace()
    for (kind in listOf("rbf", "matern")) {
        val errors = stressFactor(kind, order, workspace)
        check(errors.all { it < 1e-13 }) { "stress $kind:$order errors=${errors.toList()}" }
        println("stress_factor,$kind,$order,${errors.joinToString(",")}")
    }
    println("stress_sequence,operation,kernel,order,reconstruction,backward_error,relative_solution_error,absolute_logdet_error,forward_tolerance")
    for (kind in listOf("rbf", "matern")) {
        val fixture = CholeskyFixtures.extension(order - STRESS_APPENDED, STRESS_APPENDED, kind)
        val extended = fixture.base.factorCholesky(workspace).extended(fixture.cross, fixture.diagonal, workspace)
        checkStressSequence("extension", kind, order, compareFactors(fixture.full, extended), tolerance)
    }
    for (kind in listOf("rbf", "matern")) {
        val matrix = CholeskyFixtures.matrix("$kind:$order")
        var factor = matrix.factorCholesky(workspace)
        val updates = CholeskyFixtures.right(order, 8)
        for (column in 0 until updates.cols) {
            val update = DoubleArray(order) { updates[it, column] }
            factor = factor.rankUpdated(update, workspace)
            CholeskyFixtures.addOuterProduct(matrix, update)
        }
        checkStressSequence("update", kind, order, compareFactors(matrix, factor), tolerance)
    }
}

private const val STRESS_APPENDED = 16

private fun stressFactor(kind: String, order: Int, workspace: Workspace): DoubleArray {
    val matrix = CholeskyFixtures.matrix("$kind:$order")
    val factor = matrix.factorCholesky()
    val lower = factor.lowerFactor()
    val workspaceDifference = maxRelativeDifference(matrix.factorCholesky(workspace).lowerFactor(), lower)
    val reconstruction = relativeReconstructionError(matrix, lower)
    lower.values.fill(Double.NaN)
    val right = CholeskyFixtures.right(order, 16)
    val solution = factor.solve(right)
    val out = DenseMatrix.zero(order, 16)
    check(factor.solveInto(right, out, workspace) === out) { "solveInto must return its destination" }
    val aliased = DenseMatrix.wrap(order, 16, right.values.copyOf())
    check(factor.solveInto(aliased, aliased, workspace) === aliased) { "aliased solveInto must return its destination" }
    check(right.values.contentEquals(CholeskyFixtures.right(order, 16).values)) { "solve modified its right-hand side" }
    check(matrix.values.contentEquals(CholeskyFixtures.matrix("$kind:$order").values)) { "factorization modified its input" }
    return doubleArrayOf(
        reconstruction,
        solveBackwardError(matrix, right, solution),
        solveBackwardError(matrix, right, out),
        solveBackwardError(matrix, right, aliased),
        workspaceDifference,
    )
}

private fun checkStressSequence(operation: String, kind: String, order: Int, errors: DoubleArray, tolerance: Double) {
    check(errors[0] < 1e-13 && errors[1] < 1e-13 && errors[2] < tolerance && errors[3] < tolerance) {
        "stress $operation $kind:$order errors=${errors.toList()} tolerance=$tolerance"
    }
    println("stress_sequence,$operation,$kind,$order,${errors.joinToString(",")},$tolerance")
}

private fun maxRelativeDifference(actual: DenseMatrix, expected: DenseMatrix): Double {
    var difference = 0.0
    var magnitude = 0.0
    for (i in expected.values.indices) {
        difference = maxOf(difference, abs(actual.values[i] - expected.values[i]))
        magnitude = maxOf(magnitude, abs(expected.values[i]))
    }
    return difference / magnitude
}

private fun compareFactors(matrix: DenseMatrix, factor: CholeskyFactor): DoubleArray {
    val reference = matrix.factorCholesky()
    val right = CholeskyFixtures.right(matrix.rows, 4)
    val expected = reference.solve(right)
    val out = DenseMatrix.zero(right.rows, right.cols)
    val solution = factor.solveInto(right, out, Workspace())
    var difference = 0.0
    var magnitude = 0.0
    for (i in solution.values.indices) {
        difference = maxOf(difference, abs(solution.values[i] - expected.values[i]))
        magnitude = maxOf(magnitude, abs(expected.values[i]))
    }
    return doubleArrayOf(
        relativeReconstructionError(matrix, factor.lowerFactor()),
        solveBackwardError(matrix, right, solution),
        if (magnitude == 0.0) difference else difference / magnitude,
        abs(factor.logDeterminant() - reference.logDeterminant()),
    )
}

// Column-major traversal keeps these cubic/quadratic oracles cache-friendly at stress orders. Each entry
// still accumulates in ascending index order, so results match the entrywise definition bit for bit.
private fun relativeReconstructionError(matrix: DenseMatrix, lower: DenseMatrix): Double {
    val size = matrix.rows
    val actual = DoubleArray(size)
    var error = 0.0
    var scale = 0.0
    for (j in 0 until size) {
        actual.fill(0.0, j, size)
        for (k in 0..j) {
            val pivot = lower[j, k]
            for (i in j until size) actual[i] += lower[i, k] * pivot
        }
        for (i in j until size) {
            error = maxOf(error, abs(actual[i] - matrix[i, j]))
            scale = maxOf(scale, abs(matrix[i, j]))
        }
    }
    return error / scale
}

private fun solveBackwardError(matrix: DenseMatrix, right: DenseMatrix, solution: DenseMatrix): Double {
    val rowSums = DoubleArray(matrix.rows)
    for (j in 0 until matrix.cols) for (i in 0 until matrix.rows) rowSums[i] += abs(matrix[i, j])
    val matrixNorm = rowSums.max()
    val actual = DoubleArray(matrix.rows)
    var error = 0.0
    for (column in 0 until right.cols) {
        actual.fill(0.0)
        for (j in 0 until matrix.cols) {
            val coefficient = solution[j, column]
            for (i in 0 until matrix.rows) actual[i] += matrix[i, j] * coefficient
        }
        var residual = 0.0
        var solutionNorm = 0.0
        var rightNorm = 0.0
        for (i in 0 until matrix.rows) {
            residual = maxOf(residual, abs(actual[i] - right[i, column]))
            solutionNorm = maxOf(solutionNorm, abs(solution[i, column]))
            rightNorm = maxOf(rightNorm, abs(right[i, column]))
        }
        error = maxOf(error, residual / (matrixNorm * solutionNorm + rightNorm))
    }
    return error
}

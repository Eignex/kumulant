package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.trsm
import kotlin.math.ln

/**
 * An owned lower Cholesky factor `L` of a symmetric positive-definite matrix `A = L Lᵀ`.
 *
 * Created by [factorCholesky]. Inputs and returned matrices never share the factor's storage.
 * Solves leave their inputs unchanged. No diagonal regularization is applied; callers decide how
 * to stabilize their model before factorization. Concurrent read-only operations are safe.
 */
class CholeskyFactor internal constructor(private val lower: DenseMatrix) {
    /** Order of the square matrix. */
    val size: Int get() = lower.rows

    /** Return an independent copy of `L`, with zeros above the diagonal. */
    fun lowerFactor(): DenseMatrix = DenseMatrix.wrap(size, size, lower.values.copyOf())

    /** Natural logarithm of the determinant of `A`; zero for an empty matrix. */
    fun logDeterminant(): Double {
        var result = 0.0
        for (i in 0 until size) result += ln(lower[i, i])
        return 2.0 * result
    }

    /** Solve `A x = right`, returning an independent vector. */
    fun solve(right: DoubleArray): DoubleArray = lower.choleskySolveInto(right, DoubleArray(size))

    /** Solve `A X = right`, with one right-hand side per column. */
    fun solve(right: DenseMatrix): DenseMatrix {
        val result = solveLower(right)
        lower.trsm(result, lower = true, transpose = true)
        return result
    }

    /** Solve `L X = right`, or `Lᵀ X = right` when [transpose] is true, into an independent matrix. */
    fun solveLower(right: DenseMatrix, transpose: Boolean = false): DenseMatrix {
        require(right.rows == size) { "right-hand side has ${right.rows} rows, expected $size" }
        val result = DenseMatrix.wrap(right.rows, right.cols, right.values.copyOf())
        lower.trsm(result, lower = true, transpose = transpose)
        return result
    }
}

/**
 * Factor the lower triangle of this square symmetric matrix as `A = L Lᵀ`.
 *
 * The upper triangle is ignored, the input is unchanged, and the result owns its storage.
 * Empty matrices are supported. No jitter or pivot correction is applied.
 *
 * @throws IllegalArgumentException If the matrix is not square or its lower triangle is nonfinite.
 * @throws NotPositiveDefinite If a corrected pivot is nonpositive or the factor cannot be represented
 * with finite entries.
 */
fun DenseMatrix.factorCholesky(): CholeskyFactor {
    require(rows == cols) { "cholesky requires a square matrix" }
    for (j in 0 until cols) {
        for (i in j until rows) {
            require(this[i, j].isFinite()) { "cholesky requires a finite lower triangle" }
        }
    }
    val factor = cholesky()
    for (j in 0 until cols) {
        for (i in j until rows) {
            if (!factor[i, j].isFinite()) {
                throw NotPositiveDefinite(j, factor[j, j], "nonfinite Cholesky factor in column $j")
            }
        }
    }
    return CholeskyFactor(factor)
}

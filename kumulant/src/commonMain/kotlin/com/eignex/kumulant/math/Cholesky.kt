// math convention: the Cholesky factor is L, as everywhere it is written down.
@file:Suppress("PropertyName")

package com.eignex.kumulant.math

import com.eignex.koblas.Workspace
import com.eignex.koblas.borrow
import com.eignex.koblas.copy
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.dense.trsv
import com.eignex.koblas.norm2
import kotlin.math.absoluteValue
import kotlin.math.sqrt

/**
 * In-place Cholesky rank-1 downdate of a lower-triangular factor: modifies [this] so that the
 * matrix `A = L·Lᵀ` it represents becomes `A - x·xᵀ`. Returns `0.0` on success, or a positive
 * "norm" value when the downdate would leave the matrix outside the positive-definite cone; the
 * caller then repairs via a fresh decomposition or takes a smaller step.
 *
 * koblas covers a defined BLAS/LAPACK subset and the rank-1 factor update is outside it, so it
 * lives here, with the one caller that needs it: [com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat]
 * tracks the posterior covariance factor across observations instead of refactorizing per update.
 *
 * Solve `L·s = x` by forward substitution; if `‖s‖ < 1` the downdate stays SPD. Then apply Givens
 * rotations to the rows of L to absorb `s` without breaking triangularity. The rotation loop is
 * carried in `xx` and stays scalar; the substitution goes through the backend's `trsv`.
 */
internal fun F64DenseMatrix.choleskyDowndateInPlace(x: F64VectorLike): Double = choleskyDowndateInPlace(x, null)

/** Workspace-taking form used by repeated Bayesian updates. */
internal fun F64DenseMatrix.choleskyDowndateInPlace(x: F64VectorLike, workspace: Workspace?): Double {
    require(rows == cols) { "choleskyDowndateInPlace requires a square matrix; got ${rows}x$cols" }
    require(rows == x.size) { "x size ${x.size} must match matrix dim $rows" }
    if (rows == 0) return 0.0 // an empty downdate stays in the cone trivially
    val n = rows
    val L = data

    // Scatter rather than index x: the substitution needs every slot of a dense buffer, and reading
    // those one at a time costs a binary search per slot on a sparse x, where the scatter walks only
    // its stored entries.
    return workspace.borrow(n) { s ->
        copy(x, F64DenseVector.wrap(s))
        trsv(s, lower = true)

        val norm = F64DenseVector.wrap(s).norm2()
        // Negated so a NaN norm bails too: falling through would write NaN across the whole factor
        // and still report success.
        if (!(norm > 0.0 && norm < 1.0)) return@borrow norm

        workspace.borrow(n) { c ->
            var alpha = sqrt(1.0 - norm * norm)
            for (ii in 0 until n) {
                val i = n - ii - 1
                val scale = alpha + s[i].absoluteValue
                val a = alpha / scale
                val b = s[i] / scale
                val nrm = sqrt(a * a + b * b)
                c[i] = a / nrm
                s[i] = b / nrm
                alpha = scale * nrm
            }
            // Apply the rotations along the rows of L; entry (j, i) of the column-major backing is at
            // `j + i * n`.
            for (j in 0 until n) {
                var xx = 0.0
                for (i in j downTo 0) {
                    val idx = j + i * n
                    val t = c[i] * xx + s[i] * L[idx]
                    L[idx] = c[i] * L[idx] - s[i] * xx
                    xx = t
                }
            }
        }
        0.0
    }
}

/**
 * Zero the strict upper triangle of a Cholesky factor. koblas only promises the lower triangle of
 * `F64CholeskyDecomposition.l`, leaving whatever the backend wrote above the diagonal, so a factor
 * that gets stored in a snapshot is cleaned first: the snapshots are compared and serialised, and
 * two mathematically equal posteriors have to agree entry for entry.
 */
internal fun F64DenseMatrix.zeroUpperTriangle(): F64DenseMatrix {
    for (j in 1 until cols) {
        for (i in 0 until minOf(j, rows)) this[i, j] = 0.0
    }
    return this
}

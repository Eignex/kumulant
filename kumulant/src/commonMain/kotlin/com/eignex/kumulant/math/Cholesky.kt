// math convention: the Cholesky factor is L, as everywhere it is written down.
@file:Suppress("PropertyName")

package com.eignex.kumulant.math

import com.eignex.koblas.Workspace
import com.eignex.koblas.borrow
import com.eignex.koblas.copy
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import kotlin.math.hypot
import kotlin.math.sqrt

/**
 * In-place Cholesky rank-1 update of a lower-triangular factor: modifies [this] so that the matrix
 * `A = L·Lᵀ` it represents becomes `A + sigma·x·xᵀ`. [x] is left untouched, and [sigma] must be
 * non-negative, which is the whole point of the form: a rank-1 update of a positive-definite matrix
 * stays positive definite, so there is no failure mode to report. A negative `sigma` would be a
 * downdate, which can leave the cone and is not offered here.
 *
 * koblas covers a defined BLAS/LAPACK subset and the rank-1 factor update is outside it, so it
 * lives here, with the one caller that needs it: [com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat]
 * tracks the posterior precision factor across observations instead of refactorizing per update.
 *
 * Each column takes the plane rotation that zeroes the residual against the pivot and applies it
 * down the column: `L(i,k)` absorbs part of the residual and the rest is carried into what the
 * trailing submatrix has to take. Rotating rather than dividing by the pivot is what makes a zero
 * or tiny pivot harmless, and the rotation comes off [hypot], which rescales so a pivot and
 * residual that would both square to infinity still rotate correctly.
 */
internal fun F64DenseMatrix.choleskyUpdateInPlace(
    x: F64VectorLike,
    sigma: Double = 1.0,
    workspace: Workspace? = null,
) {
    require(rows == cols) { "choleskyUpdateInPlace requires a square matrix; got ${rows}x$cols" }
    require(rows == x.size) { "x size ${x.size} must match matrix dim $rows" }
    require(sigma >= 0.0) { "choleskyUpdateInPlace requires a non-negative sigma; got $sigma" }
    if (rows == 0 || sigma == 0.0) return
    val n = rows
    val L = data

    // The rotations consume the residual, so x is scattered into scratch rather than indexed: the
    // sweep needs every slot of a dense buffer, and reading those one at a time costs a binary
    // search per slot on a sparse x, where the scatter walks only its stored entries.
    workspace.borrow(n) { residual ->
        copy(x, F64DenseVector.wrap(residual))
        val scale = sqrt(sigma)
        if (scale != 1.0) {
            for (i in 0 until n) residual[i] = scale * residual[i]
        }
        for (k in 0 until n) {
            // Entry (i, k) of the column-major backing is at `i + k * n`, so a column is contiguous.
            val pivot = k + k * n
            val diagonal = L[pivot]
            val head = residual[k]
            // hypot rescales, so a pivot and residual that would both square to infinity still
            // rotate. It is also unsigned, which keeps the factor's diagonal positive; koblas's
            // rotg follows Netlib and signs after the larger input, flipping whole columns.
            val r = hypot(diagonal, head)
            if (r == 0.0) continue
            val c = diagonal / r
            val s = head / r
            L[pivot] = r
            if (s == 0.0) continue
            for (i in k + 1 until n) {
                val idx = i + k * n
                val lik = L[idx]
                val xi = residual[i]
                L[idx] = c * lik + s * xi
                residual[i] = c * xi - s * lik
            }
        }
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

package com.eignex.kumulant.math

import kotlin.math.absoluteValue
import kotlin.math.sqrt

/**
 * Cholesky helpers operating directly on the flat-`DoubleArray` backing of
 * [DenseMatrix], in Double precision. Internal — these are kumulant-implementation
 * utilities used by the regression stats; they aren't part of the public matrix
 * surface.
 *
 * **Convention.** All functions in this file use the **lower-triangular** Cholesky
 * factor `L` such that `A = L · Lᵀ`. Entry `(i, k)` for `k ≤ i` lives at
 * `data[i · cols + k]`; entries with `k > i` are not read or written.
 *
 * **Performance.** The hot inner loops (decomposition, forward substitution) reduce
 * to [denseDot] / [denseAxpy] on contiguous row runs — SIMD on JVM, scalar
 * elsewhere. The Givens rotation step in [choleskyDowndateInPlace] has a
 * loop-carried dependency and stays scalar. Back substitution in [solveSpd]
 * walks a strided column and also stays scalar; the forward half gets SIMD.
 */

/**
 * Lower-triangular Cholesky decomposition `A = L · Lᵀ`, returned as a fresh matrix.
 * Falls back to a small positive diagonal entry when [this] is not strictly
 * positive-definite — a regularised result is friendlier than a crash for the
 * online stats that call this on drifting precision matrices.
 */
internal fun MatrixView.cholesky(): DenseMatrix {
    require(rows == cols) { "cholesky requires a square matrix; got ${rows}x${cols}" }
    val n = rows
    val L = DenseMatrix(n, n)
    val Ld = L.data
    for (i in 0 until n) {
        val rowI = i * n
        for (j in 0..i) {
            val rowJ = j * n
            val sum = denseDot(Ld, rowI, Ld, rowJ, j)
            if (i == j) Ld[rowI + i] = sqrt(this[i, i] - sum)
            else Ld[rowI + j] = (this[i, j] - sum) / Ld[rowJ + j]
        }
        if (Ld[rowI + i] <= 0.0 || Ld[rowI + i].isNaN()) Ld[rowI + i] = 1e-5
    }
    return L
}

/**
 * In-place Cholesky downdate of a lower-triangular factor: modifies [this] so that
 * the matrix `A = L · Lᵀ` it represents becomes `A - x · xᵀ`. Returns `0.0` on
 * success, or a positive "norm" value signalling the downdate would leave the
 * matrix outside the positive-definite cone — the caller must repair via a fresh
 * decomposition or take a smaller step.
 *
 * Algorithm: solve `L · s = x` via forward substitution; if `‖s‖ < 1` the downdate
 * stays SPD. Apply a sequence of Givens rotations to the rows of L (the natural
 * direction for lower-triangular storage) so that `s` is absorbed without
 * destroying the triangular structure.
 */
internal fun DenseMatrix.choleskyDowndateInPlace(x: VectorView): Double {
    require(rows == cols) { "choleskyDowndateInPlace requires a square matrix; got ${rows}x${cols}" }
    require(rows == x.size) { "x size ${x.size} must match matrix dim $rows" }
    val L = data
    val n = rows
    val s = DoubleArray(n)
    val c = DoubleArray(n)

    // Solve L · s = x by forward substitution. Inner sum is a contiguous dot product.
    s[0] = x[0] / L[0]
    for (i in 1 until n) {
        val rowI = i * n
        val sum = denseDot(L, rowI, s, 0, i)
        s[i] = (x[i] - sum) / L[rowI + i]
    }

    var norm = 0.0
    for (v in s) norm += v * v
    norm = sqrt(norm)
    if (norm <= 0.0 || norm >= 1.0) return norm

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
    // Apply rotations along rows of L. Loop-carried in xx — stays scalar.
    for (j in 0 until n) {
        val rowJ = j * n
        var xx = 0.0
        for (ii in 0..j) {
            val i = j - ii
            val idx = rowJ + i
            val t = c[i] * xx + s[i] * L[idx]
            L[idx] = c[i] * L[idx] - s[i] * xx
            xx = t
        }
    }
    return 0.0
}

/**
 * Solve `A · x = b` for `x`, given `L = chol(A)` (lower-triangular, `A = L · Lᵀ`).
 * Allocates a fresh result vector; [b] is not modified.
 *
 * Forward substitution `L · y = b` runs over contiguous row data and uses [denseDot].
 * Back substitution `Lᵀ · x = y` walks a strided column and stays scalar.
 */
internal fun solveSpd(L: DenseMatrix, b: DoubleArray): DoubleArray {
    val n = L.rows
    require(b.size == n) { "solveSpd: b size ${b.size}, expected $n" }
    val Ld = L.data
    val y = DoubleArray(n)
    for (i in 0 until n) {
        val rowI = i * n
        val sum = denseDot(Ld, rowI, y, 0, i)
        y[i] = (b[i] - sum) / Ld[rowI + i]
    }
    val x = DoubleArray(n)
    for (ii in 0 until n) {
        val i = n - 1 - ii
        var sum = y[i]
        for (k in i + 1 until n) sum -= Ld[k * n + i] * x[k]
        x[i] = sum / Ld[i * n + i]
    }
    return x
}

/** Invert an SPD matrix from its Cholesky factor: returns `A⁻¹` given `L = chol(A)`. */
internal fun invertSpd(L: DenseMatrix): DenseMatrix {
    val n = L.rows
    val inv = DenseMatrix(n, n)
    val e = DoubleArray(n)
    for (j in 0 until n) {
        for (k in 0 until n) e[k] = 0.0
        e[j] = 1.0
        val col = solveSpd(L, e)
        for (i in 0 until n) inv[i, j] = col[i]
    }
    return inv
}

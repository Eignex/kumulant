@file:Suppress("VariableNaming", "FunctionParameterNaming")

package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.koblas.borrow
import com.eignex.koblas.dense.DenseVectorKernels
import com.eignex.koblas.koblas
import com.eignex.koblas.trsv
import kotlin.math.abs
import kotlin.math.hypot
import kotlin.math.min
import kotlin.math.sqrt

// Adapted from Eignex/koblas ReferenceCholesky.kt at cfdef98439f7baff0febc7a0892f16734bf55315.
// Apache-2.0; module-internal API.
private const val CHOLESKY_BLOCK = 64

internal sealed interface CholeskyPolicy {
    data object Strict : CholeskyPolicy

    data class Regularize(val minimumPivot: Double = 1e-10) : CholeskyPolicy {
        init {
            require(minimumPivot > 0.0 && minimumPivot.isFinite()) {
                "minimumPivot must be positive and finite, got $minimumPivot"
            }
        }
    }
}

internal class NotPositiveDefinite(val pivotIndex: Int, val pivot: Double, message: String) :
    IllegalArgumentException(message)

internal fun DenseMatrix.choleskyRankUpdate(v: DoubleArray, sigma: Double, workspace: Workspace? = null) {
    require(rows == cols) { "cholesky factor must be square" }
    require(v.size == rows) { "update vector has ${v.size} entries, expected $rows" }
    require(sigma >= 0.0 && sigma.isFinite()) { "sigma must be non-negative and finite, got $sigma" }
    val n = rows
    if (n == 0 || sigma == 0.0) return
    val ld = data
    val kernels = koblas.vectorKernels
    val scale = sqrt(sigma)
    workspace.borrow(n) { x ->
        v.copyInto(x)
        if (scale != 1.0) kernels.scale(x, 0, scale, n)
        for (k in 0 until n) {
            val base = k + k * n
            val diagonal = ld[base]
            // Inline the rotation to avoid allocating a Givens object per coordinate. A positive
            // hypot keeps the factor diagonal non-negative and avoids squaring overflow or underflow.
            val entry = x[k]
            val r = hypot(diagonal, entry)
            if (r != 0.0) {
                ld[base] = r
                val len = n - k - 1
                // Two divisions rather than a reciprocal and two multiplies: this rotation sets the
                // factor's numerical quality, and an extra rounding per entry compounds across n of them.
                if (len > 0) kernels.rot(ld, base + 1, x, k + 1, len, diagonal / r, entry / r)
            }
        }
    }
}

internal fun DenseMatrix.choleskySolveInto(b: DoubleArray, out: DoubleArray): DoubleArray {
    require(rows == cols) { "cholesky factor must be square" }
    require(b.size == rows && out.size == rows) { "solve requires vectors of size $rows" }
    if (out !== b) b.copyInto(out)
    trsv(out, lower = true)
    trsv(out, lower = true, transpose = true)
    return out
}

internal fun DenseMatrix.choleskyInverse(workspace: Workspace? = null): DenseMatrix =
    choleskyInvertInto(DenseMatrix.zero(rows, cols), workspace)

internal fun DenseMatrix.choleskyInvertInto(out: DenseMatrix, workspace: Workspace? = null): DenseMatrix {
    require(rows == cols) { "cholesky factor must be square" }
    require(out.rows == rows && out.cols == rows) { "inverse destination must be ${rows}x$rows" }
    require(out.data !== data) { "inverse destination must not share the factor's storage" }
    val n = rows
    val ld = data
    val kernels = koblas.vectorKernels
    val invd = out.data
    workspace.borrow(n) { y ->
        for (j in 0 until n) {
            y.fill(0.0, j, n)
            y[j] = 1.0
            for (c in j until n) {
                val base = c + c * n
                val yc = y[c] / ld[base]
                y[c] = yc
                if (yc != 0.0) kernels.axpy(y, c + 1, -yc, ld, base + 1, n - c - 1)
            }
            for (i in n - 1 downTo j) {
                val base = i + i * n
                y[i] = (y[i] - kernels.dot(ld, base + 1, y, i + 1, n - i - 1)) / ld[base]
            }
            for (i in j until n) {
                invd[i + j * n] = y[i]
                invd[j + i * n] = y[i]
            }
        }
    }
    return out
}

internal fun DenseMatrix.cholesky(
    policy: CholeskyPolicy = CholeskyPolicy.Strict,
    workspace: Workspace? = null,
): DenseMatrix = choleskyInto(DenseMatrix.zero(rows, cols), policy, workspace)

internal fun DenseMatrix.choleskyInto(
    out: DenseMatrix,
    policy: CholeskyPolicy = CholeskyPolicy.Strict,
    workspace: Workspace? = null,
): DenseMatrix {
    require(rows == cols) { "cholesky requires a square matrix" }
    require(out.rows == rows && out.cols == rows) { "cholesky destination must be ${rows}x$rows" }
    val n = rows
    val kernels = koblas.choleskyKernels()
    val ld = out.data
    // A reused destination arrives holding the previous factor where a fresh one arrives zeroed, so the
    // strict upper triangle is cleared rather than inherited.
    for (j in 0 until n) {
        ld.fill(0.0, j * n, j * n + j)
        if (data !== ld) data.copyInto(ld, j + j * n, j + j * n, (j + 1) * n)
    }
    if (n <= CHOLESKY_BLOCK) {
        factorBlockColumn(kernels.vector, ld, n, 0, n, policy)
        return out
    }
    val minimumPivot = (policy as? CholeskyPolicy.Regularize)?.minimumPivot ?: 0.0
    val block = min(CHOLESKY_BLOCK, n / 4)
    var start = 0
    while (start < n) {
        val width = min(block, n - start)
        if (!tryCholeskyBlock(kernels, out, start, width, minimumPivot, workspace)) {
            // Regularization needs the entire corrected column to bound its multipliers.
            factorBlockColumn(kernels.vector, ld, n, start, width, policy)
            if (columnsAreFinite(ld, n, start, width)) {
                updateCholeskyTrailing(kernels, out, start, width, workspace)
            } else {
                // A zero multiplier must skip infinite column entries instead of forming 0 * infinity.
                subtractTrailingColumns(kernels.vector, ld, n, start, width)
            }
        }
        start += width
    }
    return out
}

private fun columnsAreFinite(data: DoubleArray, n: Int, start: Int, width: Int): Boolean {
    for (j in start until start + width) {
        for (i in j until n) if (!data[i + j * n].isFinite()) return false
    }
    return true
}

private fun subtractTrailingColumns(kernels: DenseVectorKernels, data: DoubleArray, n: Int, start: Int, width: Int) {
    for (p in start until start + width) {
        for (j in start + width until n) {
            val value = data[j + p * n]
            if (value != 0.0) kernels.axpy(data, j + j * n, -value, data, j + p * n, n - j)
        }
    }
}

private fun factorBlockColumn(
    kernels: DenseVectorKernels,
    ld: DoubleArray,
    n: Int,
    start: Int,
    width: Int,
    policy: CholeskyPolicy,
) {
    for (j in start until start + width) {
        val base = j + j * n
        val len = n - j
        for (p in start until j) {
            val f = ld[j + p * n]
            if (f != 0.0) kernels.axpy(ld, base, -f, ld, j + p * n, len)
        }
        val pivot = ld[base]
        // A NaN is corrupt input, not indefiniteness, and no floor repairs it: the tail stays NaN while the
        // diagonal reads clean, so a finite diagonal would conceal the corruption. Refused under every policy.
        if (pivot.isNaN()) {
            throw NotPositiveDefinite(j, pivot, "matrix has a NaN pivot at $j, so it cannot be factored")
        }
        val floor = (policy as? CholeskyPolicy.Regularize)?.minimumPivot
        if (floor == null) {
            if (pivot <= 0.0) {
                throw NotPositiveDefinite(
                    j,
                    pivot,
                    "matrix is not positive-definite at pivot $j (diagonal=$pivot); pass " +
                        "CholeskyPolicy.Regularize to factor a nearby matrix instead",
                )
            }
            ld[base] = sqrt(pivot)
        } else if (pivot < floor) {
            ld[base] = sqrt(regularizedPivot(ld, base, len, floor))
        } else {
            ld[base] = sqrt(pivot)
        }
        val diag = ld[base]
        for (i in base + 1 until base + len) ld[i] = ld[i] / diag
    }
}

// Bounding the column multipliers by one avoids amplifying the trailing matrix by 1/floor.
private fun regularizedPivot(ld: DoubleArray, base: Int, len: Int, floor: Double): Double {
    var largest = 0.0
    for (i in base + 1 until base + len) {
        val magnitude = abs(ld[i])
        if (magnitude > largest) largest = magnitude
    }
    return maxOf(floor, largest * largest)
}

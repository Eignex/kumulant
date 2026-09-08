@file:Suppress("VariableNaming", "FunctionParameterNaming")

package com.eignex.kumulant.math

import com.eignex.koblas.Workspace
import com.eignex.koblas.borrow
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.dense.F64Kernels
import com.eignex.koblas.dense.trsv
import com.eignex.koblas.koblas
import kotlin.math.abs
import kotlin.math.hypot
import kotlin.math.min
import kotlin.math.sqrt

// Adapted from Eignex/koblas ReferenceCholesky.kt and ReferenceLevel3.kt at cfdef98439f7baff0febc7a0892f16734bf55315.
// Apache-2.0; module-internal API and a Cholesky-only specialization of the blocked product.
private const val CHOLESKY_BLOCK = 64
private const val PRODUCT_ROWS = 256
private const val PRODUCT_COLUMNS = 8
private const val PRODUCT_DEPTH = 128

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

internal fun F64DenseMatrix.choleskyRankUpdate(v: DoubleArray, sigma: Double, workspace: Workspace? = null) {
    require(rows == cols) { "cholesky factor must be square" }
    require(v.size == rows) { "update vector has ${v.size} entries, expected $rows" }
    require(sigma >= 0.0 && sigma.isFinite()) { "sigma must be non-negative and finite, got $sigma" }
    val n = rows
    if (n == 0 || sigma == 0.0) return
    val ld = data
    val kernels = koblas.kernels
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

internal fun F64DenseMatrix.choleskySolveInto(b: DoubleArray, out: DoubleArray): DoubleArray {
    require(rows == cols) { "cholesky factor must be square" }
    require(b.size == rows && out.size == rows) { "solve requires vectors of size $rows" }
    if (out !== b) b.copyInto(out)
    trsv(out, lower = true)
    trsv(out, lower = true, transpose = true)
    return out
}

internal fun F64DenseMatrix.choleskyInverse(workspace: Workspace? = null): F64DenseMatrix =
    choleskyInvertInto(F64DenseMatrix.zero(rows, cols), workspace)

internal fun F64DenseMatrix.choleskyInvertInto(out: F64DenseMatrix, workspace: Workspace? = null): F64DenseMatrix {
    require(rows == cols) { "cholesky factor must be square" }
    require(out.rows == rows && out.cols == rows) { "inverse destination must be ${rows}x$rows" }
    require(out.data !== data) { "inverse destination must not share the factor's storage" }
    val n = rows
    val ld = data
    val kernels = koblas.kernels
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

internal fun F64DenseMatrix.cholesky(policy: CholeskyPolicy = CholeskyPolicy.Strict): F64DenseMatrix =
    choleskyInto(F64DenseMatrix.zero(rows, cols), policy)

internal fun F64DenseMatrix.choleskyInto(
    out: F64DenseMatrix,
    policy: CholeskyPolicy = CholeskyPolicy.Strict,
): F64DenseMatrix {
    require(rows == cols) { "cholesky requires a square matrix" }
    require(out.rows == rows && out.cols == rows) { "cholesky destination must be ${rows}x$rows" }
    val n = rows
    val kernels = koblas.kernels
    val ld = out.data
    // A reused destination arrives holding the previous factor where a fresh one arrives zeroed, so the
    // strict upper triangle is cleared rather than inherited.
    for (j in 0 until n) {
        ld.fill(0.0, j * n, j * n + j)
        if (data !== ld) data.copyInto(ld, j + j * n, j + j * n, (j + 1) * n)
    }
    // Left-looking blocked Cholesky. Each block column first takes what every earlier block owes it as one
    // level-3 product, then finishes column by column against the few columns inside the block. Gathering
    // one column at a time instead is a level-1 axpy per (column, earlier column) pair, which streams
    // n^3/6 doubles for n^3/3 flops and leaves the factorization bandwidth-bound.
    // One pack buffer for the whole factorization rather than one per block column. The seam takes no
    // workspace, and a matrix that fits in a single block never gathers at all.
    val packed = if (n > CHOLESKY_BLOCK) DoubleArray(CHOLESKY_BLOCK * n) else null
    var blockStart = 0
    while (blockStart < n) {
        val width = min(CHOLESKY_BLOCK, n - blockStart)
        val height = n - blockStart
        if (blockStart > 0) {
            gatherEarlierBlocks(kernels, ld, n, blockStart, width, height, packed!!)
        }
        factorBlockColumn(kernels, ld, n, blockStart, width, policy)
        blockStart += width
    }
    return out
}

@Suppress("LongParameterList") // the factor, its shape, the block being gathered into, and scratch
private fun gatherEarlierBlocks(
    kernels: F64Kernels,
    ld: DoubleArray,
    n: Int,
    start: Int,
    width: Int,
    height: Int,
    packed: DoubleArray,
) {
    for (t in 0 until width) {
        for (p in 0 until start) packed[p + t * start] = ld[start + t + p * n]
    }
    var column = 0
    while (column < width) {
        val columnEnd = min(column + PRODUCT_COLUMNS, width)
        var inner = 0
        while (inner < start) {
            val innerEnd = min(inner + PRODUCT_DEPTH, start)
            var row = 0
            while (row < height) {
                val length = min(row + PRODUCT_ROWS, height) - row
                for (p in inner until innerEnd) {
                    val source = start + row + p * n
                    for (j in column until columnEnd) {
                        val value = packed[p + j * start]
                        if (value != 0.0) {
                            kernels.axpy(ld, start + start * n + row + j * n, -value, ld, source, length)
                        }
                    }
                }
                row += length
            }
            inner = innerEnd
        }
        column = columnEnd
    }
    // The product covers the whole diagonal block, including the strict upper half of it that the column
    // sweep neither writes nor reads. Left there it would surface as a nonzero above the factor's diagonal.
    for (t in 1 until width) {
        val column = start + t
        ld.fill(0.0, start + column * n, column + column * n)
    }
}

private fun factorBlockColumn(
    kernels: F64Kernels,
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

package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.koblas.borrow
import com.eignex.koblas.koblas
import com.eignex.koblas.trsm
import com.eignex.koblas.trsv
import kotlin.math.ln

/**
 * An owned lower Cholesky factor `L` of a symmetric positive-definite matrix `A = L Lᵀ`.
 *
 * Created by [factorCholesky]. Inputs and returned matrices never share the factor's storage.
 * Allocating solves leave inputs unchanged. Destination solves may overwrite a supplied right-hand
 * side. No diagonal regularization is applied; callers decide how to stabilize their model before
 * factorization. Concurrent operations are safe with independent destinations and workspaces.
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

    /**
     * Solve `A x = right` into [out], which may be [right], and return [out].
     *
     * Both vectors must have [size] entries. Other inputs and the factor are unchanged.
     * [workspace] belongs to this invocation; concurrent calls must use separate workspaces.
     */
    fun solveInto(right: DoubleArray, out: DoubleArray, workspace: Workspace? = null): DoubleArray =
        lower.choleskySolveInto(right, out, workspace)

    /** Solve `A X = right`, with one right-hand side per column. */
    fun solve(right: DenseMatrix): DenseMatrix {
        val result = solveLower(right)
        solveTriangular(result, transpose = true, workspace = null)
        return result
    }

    /**
     * Solve `A X = right` into [out], which may share [right]'s storage, and return [out].
     *
     * Both matrices must have [size] rows and the same column count. The factor is unchanged.
     * [workspace] belongs to this invocation; concurrent calls must use separate workspaces.
     */
    fun solveInto(right: DenseMatrix, out: DenseMatrix, workspace: Workspace? = null): DenseMatrix {
        solveLowerInto(right, out, workspace = workspace)
        solveTriangular(out, transpose = true, workspace)
        return out
    }

    /** Solve `L X = right`, or `Lᵀ X = right` when [transpose] is true, into an independent matrix. */
    fun solveLower(right: DenseMatrix, transpose: Boolean = false): DenseMatrix {
        require(right.rows == size) { "right-hand side has ${right.rows} rows, expected $size" }
        val result = DenseMatrix.wrap(right.rows, right.cols, right.values.copyOf())
        solveTriangular(result, transpose, workspace = null)
        return result
    }

    /**
     * Solve `L X = right`, or `Lᵀ X = right` when [transpose], into [out] and return [out].
     *
     * Both matrices must have [size] rows and the same column count. [out] may share [right]'s
     * storage. The factor is unchanged. [workspace] belongs to one invocation at a time.
     */
    fun solveLowerInto(
        right: DenseMatrix,
        out: DenseMatrix,
        transpose: Boolean = false,
        workspace: Workspace? = null,
    ): DenseMatrix {
        require(right.rows == size) { "right-hand side has ${right.rows} rows, expected $size" }
        require(out.rows == size && out.cols == right.cols) { "solve destination must be ${size}x${right.cols}" }
        if (out.values !== right.values) right.values.copyInto(out.values)
        solveTriangular(out, transpose, workspace)
        return out
    }

    private fun solveTriangular(out: DenseMatrix, transpose: Boolean, workspace: Workspace?) {
        if (out.cols == 1) {
            lower.trsv(out.values, lower = true, transpose = transpose, workspace = workspace)
        } else {
            lower.trsm(out, lower = true, transpose = transpose, workspace = workspace)
        }
    }

    /**
     * Return an owned factor of `A + update updateᵀ`, leaving this factor and [update] unchanged.
     *
     * [update] must contain [size] finite entries. No regularization is applied.
     * [workspace] lends rotation scratch and belongs to one invocation at a time.
     *
     * @throws IllegalArgumentException If the update has the wrong size or nonfinite entries.
     * @throws NotPositiveDefinite If the updated factor cannot be represented with finite entries.
     */
    fun rankUpdated(update: DoubleArray, workspace: Workspace? = null): CholeskyFactor {
        require(update.size == size) { "update vector has ${update.size} entries, expected $size" }
        require(update.all { it.isFinite() }) { "rank update requires a finite vector" }
        val result = lowerFactor()
        result.choleskyRankUpdate(update, 1.0, workspace)
        result.requireFiniteCholeskyFactor()
        return CholeskyFactor(result)
    }

    /**
     * Return an owned factor of the block matrix `[[A, cross], [crossᵀ, diagonal]]`.
     *
     * [cross] has [size] rows and one column per appended entry. [diagonal] is square with that
     * many rows; its upper triangle is ignored. This factor and both inputs are unchanged.
     * [workspace] belongs to one invocation at a time. No jitter or pivot correction is applied.
     *
     * @throws IllegalArgumentException If the shapes disagree or the selected input entries are nonfinite.
     * @throws NotPositiveDefinite If the Schur complement is not positive definite or its factor is
     * nonfinite. The failing pivot index refers to the enlarged matrix.
     */
    fun extended(cross: DenseMatrix, diagonal: DenseMatrix, workspace: Workspace? = null): CholeskyFactor {
        require(cross.rows == size) { "cross block has ${cross.rows} rows, expected $size" }
        require(
            diagonal.rows == cross.cols && diagonal.cols == cross.cols,
        ) { "diagonal block must be ${cross.cols}x${cross.cols}" }
        require(cross.values.all { it.isFinite() }) { "extension requires a finite cross block" }
        diagonal.requireCholeskyInput()
        val added = cross.cols
        return workspace.borrow(size * added) { projectedValues ->
            val projected = DenseMatrix.wrap(size, added, projectedValues)
            solveLowerInto(cross, projected, workspace = workspace)
            workspace.borrow(added * added) { schurValues ->
                diagonal.values.copyInto(schurValues)
                val schur = DenseMatrix.wrap(added, added, schurValues)
                koblas.syrk(-1.0, projected, transpose = true, 1.0, schur, lower = true, workspace = workspace)
                try {
                    schur.choleskyInto(schur, workspace = workspace)
                    schur.requireFiniteCholeskyFactor()
                } catch (failure: NotPositiveDefinite) {
                    throw failure.offsetBy(size)
                }
                val order = size + added
                val result = DenseMatrix.zero(order, order)
                for (j in 0 until size) {
                    lower.values.copyInto(result.values, j * order, j * size, (j + 1) * size)
                    for (i in 0 until added) result[size + i, j] = projected[j, i]
                }
                for (j in 0 until added) {
                    schur.values.copyInto(result.values, (size + j) * order + size, j * added, (j + 1) * added)
                }
                result.requireFiniteCholeskyFactor()
                CholeskyFactor(result)
            }
        }
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
fun DenseMatrix.factorCholesky(): CholeskyFactor = factorCholeskyWithWorkspace(null)

/**
 * Factor the lower triangle into an owned [CholeskyFactor], lending [workspace] to the numerical kernels.
 *
 * The input and failure contracts are those of [factorCholesky]. The result owns its storage;
 * [workspace] only lends scratch and belongs to one invocation at a time.
 */
fun DenseMatrix.factorCholesky(workspace: Workspace): CholeskyFactor = factorCholeskyWithWorkspace(workspace)

private fun DenseMatrix.factorCholeskyWithWorkspace(workspace: Workspace?): CholeskyFactor {
    requireCholeskyInput()
    val factor = choleskyInto(DenseMatrix.zero(rows, cols), workspace = workspace)
    factor.requireFiniteCholeskyFactor()
    return CholeskyFactor(factor)
}

private fun DenseMatrix.requireCholeskyInput() {
    require(rows == cols) { "cholesky requires a square matrix" }
    for (j in 0 until cols) {
        for (i in j until rows) {
            require(this[i, j].isFinite()) { "cholesky requires a finite lower triangle" }
        }
    }
}

private fun DenseMatrix.requireFiniteCholeskyFactor() {
    for (j in 0 until cols) {
        for (i in j until rows) {
            if (!this[i, j].isFinite()) {
                throw NotPositiveDefinite(j, this[j, j], "nonfinite Cholesky factor in column $j")
            }
        }
    }
}

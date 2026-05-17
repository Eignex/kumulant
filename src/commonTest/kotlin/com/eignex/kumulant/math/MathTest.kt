package com.eignex.kumulant.math

import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Focused tests for the internal math primitives. Every operation is exercised on
 * a non-diagonal, asymmetric example - that's the case where Cholesky convention
 * bugs and sparse/dense-dispatch mistakes show up. The regression-stat tests
 * happen to pass even with a sign or transpose error because their tolerances are
 * loose; these tests pin the math down.
 */
@Suppress("VariableNaming") // single-letter matrix/vector names track math conventions
class MathTest {

    private fun dense(vararg v: Double) = DenseVector.of(v)
    private fun sparse(size: Int, vararg pairs: Pair<Int, Double>) =
        SparseVector.of(size, pairs.map { it.first }.toIntArray(), pairs.map { it.second }.toDoubleArray())

    @Test
    fun `dot is symmetric and sparsity-agnostic`() {
        val a = dense(1.0, 2.0, 3.0, 4.0)
        val b = dense(0.5, 0.0, -1.0, 2.0)
        val bSparse = sparse(4, 0 to 0.5, 2 to -1.0, 3 to 2.0)
        val expected = 1 * 0.5 + 2 * 0 + 3 * (-1) + 4 * 2
        assertEquals(expected, a dot b, 1e-12)
        assertEquals(expected, b dot a, 1e-12)
        assertEquals(expected, a dot bSparse, 1e-12)
        assertEquals(expected, bSparse dot a, 1e-12)
        // Same vector twice (sparse x dense form) - exercises the sparse-dispatch path.
        assertEquals(0.5 * 0.5 + 0 + 1 + 4, bSparse dot b, 1e-12)
    }

    @Test
    fun `axpy adds alpha-scaled x to y for any sparsity`() {
        val y = DenseVector.of(doubleArrayOf(1.0, 2.0, 3.0))
        axpy(y, 2.0, sparse(3, 0 to 1.0, 2 to -1.0))
        assertEquals(dense(3.0, 2.0, 1.0), y)
    }

    @Test
    fun `scale mutates in place and respects identity`() {
        val v = DenseVector.of(doubleArrayOf(1.0, -2.0, 3.0))
        scale(v, 0.5)
        assertEquals(dense(0.5, -1.0, 1.5), v)
        scale(v, 1.0) // no-op
        assertEquals(dense(0.5, -1.0, 1.5), v)
    }

    @Test
    fun `matVec computes A x for dense and sparse x`() {
        // A = [[1, 2], [3, 4], [5, 6]]
        val A = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 2.0),
                doubleArrayOf(3.0, 4.0),
                doubleArrayOf(5.0, 6.0),
            )
        )
        val xDense = dense(1.0, -1.0)
        val xSparse = sparse(2, 0 to 1.0, 1 to -1.0)
        val expected = dense(1.0 * 1 + 2 * -1, 3.0 * 1 + 4 * -1, 5.0 * 1 + 6 * -1)
        assertEquals(expected, matVec(A, xDense))
        assertEquals(expected, matVec(A, xSparse))
    }

    @Test
    fun `addOuter updates a matrix with alpha x y_transpose`() {
        // M starts as identity 2x2; add 0.5 * [1,2] * [3,4]^T = 0.5 * [[3,4],[6,8]]
        val M = DenseMatrix.diagonal(2, 1.0)
        addOuter(M, 0.5, dense(1.0, 2.0), dense(3.0, 4.0))
        assertEquals(1.0 + 0.5 * 3, M[0, 0], 1e-12)
        assertEquals(0.5 * 4, M[0, 1], 1e-12)
        assertEquals(0.5 * 6, M[1, 0], 1e-12)
        assertEquals(1.0 + 0.5 * 8, M[1, 1], 1e-12)
    }

    @Test
    fun `addOuter with sparse operands only touches nonzero rows and cols`() {
        val M = DenseMatrix.diagonal(3, 0.0)
        addOuter(M, 1.0, sparse(3, 1 to 2.0), sparse(3, 0 to 3.0, 2 to 4.0))
        // Only row 1 should be nonzero; cols 0 and 2 in that row get 2*3=6 and 2*4=8.
        for (i in 0 until 3) for (j in 0 until 3) {
            val expected = if (i == 1 && j == 0) 6.0 else if (i == 1 && j == 2) 8.0 else 0.0
            assertEquals(expected, M[i, j], 1e-12, "M[$i,$j]")
        }
    }

    @Test
    fun `cholesky reconstructs A as L Lt for a non-diagonal SPD matrix`() {
        // A = [[4, 2, 0], [2, 5, 1], [0, 1, 3]] - symmetric positive-definite.
        val A = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(4.0, 2.0, 0.0),
                doubleArrayOf(2.0, 5.0, 1.0),
                doubleArrayOf(0.0, 1.0, 3.0),
            )
        )
        val L = A.cholesky()
        // Verify A == L * LT.
        for (i in 0 until 3) for (j in 0 until 3) {
            var s = 0.0
            for (k in 0..minOf(i, j)) s += L[i, k] * L[j, k]
            assertEquals(A[i, j], s, 1e-10, "L LT mismatch at [$i,$j]")
        }
        // Strict lower triangular: upper entries zero.
        for (i in 0 until 3) for (j in i + 1 until 3) assertEquals(0.0, L[i, j], "L[$i,$j] should be zero")
    }

    @Test
    fun `solveSpd inverts A b via Cholesky factor`() {
        val A = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(4.0, 2.0, 0.0),
                doubleArrayOf(2.0, 5.0, 1.0),
                doubleArrayOf(0.0, 1.0, 3.0),
            )
        )
        val L = A.cholesky()
        val b = doubleArrayOf(1.0, 0.5, -1.0)
        val x = solveSpd(L, b)
        // A * x should reproduce b.
        for (i in 0 until 3) {
            var s = 0.0
            for (j in 0 until 3) s += A[i, j] * x[j]
            assertEquals(b[i], s, 1e-10, "A*x reproduce b at $i")
        }
    }

    @Test
    fun `invertSpd produces A inverse for a non-diagonal matrix`() {
        val A = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(4.0, 2.0, 0.0),
                doubleArrayOf(2.0, 5.0, 1.0),
                doubleArrayOf(0.0, 1.0, 3.0),
            )
        )
        val L = A.cholesky()
        val Ainv = invertSpd(L)
        // A * Ainv should be identity.
        for (i in 0 until 3) for (j in 0 until 3) {
            var s = 0.0
            for (k in 0 until 3) s += A[i, k] * Ainv[k, j]
            val expected = if (i == j) 1.0 else 0.0
            assertEquals(expected, s, 1e-9, "A*Ainv mismatch at [$i,$j]")
        }
    }

    @Test
    fun `cholesky downdate then reconstruct equals A minus x xt`() {
        // Build an SPD A with enough headroom to absorb the downdate.
        val A = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(10.0, 2.0, 1.0),
                doubleArrayOf(2.0, 8.0, 3.0),
                doubleArrayOf(1.0, 3.0, 7.0),
            )
        )
        val L = A.cholesky()
        val x = doubleArrayOf(0.5, 1.0, -0.5)
        val norm = L.choleskyDowndateInPlace(DenseVector.of(x))
        assertEquals(0.0, norm, "downdate should stay in the SPD cone")
        // L_new * L_newT should equal A - x*xT.
        for (i in 0 until 3) for (j in 0 until 3) {
            var s = 0.0
            for (k in 0..minOf(i, j)) s += L[i, k] * L[j, k]
            val expected = A[i, j] - x[i] * x[j]
            assertEquals(expected, s, 1e-9, "downdate mismatch at [$i,$j]")
        }
    }

    @Test
    fun `cholesky downdate refuses to exit the SPD cone`() {
        val A = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(2.0, 0.0),
                doubleArrayOf(0.0, 2.0),
            )
        )
        val L = A.cholesky()
        // x with ||L^-1 x|| >= 1 - pick x so that x*xT swamps A.
        val norm = L.choleskyDowndateInPlace(DenseVector.of(doubleArrayOf(3.0, 0.0)))
        assertTrue(norm >= 1.0, "expected norm >= 1 for an infeasible downdate, got $norm")
    }

    @Test
    fun `cholesky strict mode throws on a non positive definite pivot`() {
        // Negative diagonal pivot - immediately rejected with regularizeNonPD=false.
        val bad = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 0.0),
                doubleArrayOf(0.0, -0.5),
            )
        )
        assertFailsWith<IllegalArgumentException> { bad.cholesky(regularizeNonPD = false) }
        // Default regularising path still succeeds (clamps the pivot to 1e-5).
        val L = bad.cholesky()
        assertTrue(L[1, 1] > 0.0)
    }

    @Test
    fun `sparse and dense matVec agree on a random spd-shaped example`() {
        val rng = Random(7)
        val n = 8
        val A = DenseMatrix(n, n).also { m ->
            for (i in 0 until n) for (j in 0 until n) m[i, j] = rng.nextDouble() * 2 - 1
        }
        // Build a sparse x that touches half the coords.
        val nz = (0 until n).filter { rng.nextBoolean() }
        val xv = DoubleArray(n)
        for (i in nz) xv[i] = rng.nextDouble() * 2 - 1
        val xDense = DenseVector.of(xv)
        val xSparse = SparseVector.of(n, nz.toIntArray(), nz.map { xv[it] }.toDoubleArray())
        val rd = matVec(A, xDense)
        val rs = matVec(A, xSparse)
        for (i in 0 until n) assertTrue(abs(rd[i] - rs[i]) < 1e-12, "matVec dense/sparse diverge at $i")
    }
}

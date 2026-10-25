package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CholeskyFactorTest {
    @Test
    fun `factor owns its input and returned lower matrix`() {
        val matrix = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, Double.NaN, 5.0))
        val original = matrix.values.copyOf()

        val factor = matrix.factorCholesky()
        val lower = factor.lowerFactor()

        assertContentEquals(original, matrix.values)
        assertContentEquals(doubleArrayOf(2.0, 1.0, 0.0, 2.0), lower.values)
        matrix.values.fill(0.0)
        lower.values.fill(0.0)
        assertContentEquals(doubleArrayOf(2.0, 1.0, 0.0, 2.0), factor.lowerFactor().values)
        assertEquals(2, factor.size)
        assertEquals(ln(16.0), factor.logDeterminant(), 1e-14)
    }

    @Test
    fun `vector and matrix systems recover their known solutions`() {
        val matrix = DenseMatrix.wrap(3, 3, doubleArrayOf(4.0, 2.0, -2.0, 2.0, 10.0, 5.0, -2.0, 5.0, 21.0))
        val factor = matrix.factorCholesky()
        val right = doubleArrayOf(10.0, 17.0, -13.0)
        val rights = DenseMatrix.wrap(3, 2, right + right.map { -it })
        val original = rights.values.copyOf()
        val expected = doubleArrayOf(1.0, 2.0, -1.0)

        val solution = factor.solve(right)
        val solutions = factor.solve(rights)

        for (i in expected.indices) {
            assertEquals(expected[i], solution[i], 1e-13)
            assertEquals(expected[i], solutions[i, 0], 1e-13)
            assertEquals(-expected[i], solutions[i, 1], 1e-13)
        }
        assertContentEquals(doubleArrayOf(10.0, 17.0, -13.0), right)
        assertContentEquals(original, rights.values)
    }

    @Test
    fun `lower triangular solves support either orientation`() {
        val matrix = DenseMatrix.wrap(3, 3, doubleArrayOf(4.0, 2.0, -2.0, 2.0, 10.0, 5.0, -2.0, 5.0, 21.0))
        val factor = matrix.factorCholesky()
        for (transpose in listOf(false, true)) {
            val right = if (transpose) doubleArrayOf(5.0, 4.0, -4.0) else doubleArrayOf(2.0, 7.0, -1.0)
            val original = right.copyOf()

            val solution = factor.solveLower(DenseMatrix.wrap(3, 1, right), transpose)

            for ((i, expected) in doubleArrayOf(1.0, 2.0, -1.0).withIndex()) {
                assertEquals(expected, solution[i, 0], 1e-13)
            }
            assertContentEquals(original, right)
        }
    }

    @Test
    fun `empty matrices and zero right hand sides are supported`() {
        val factor = DenseMatrix.zero(0, 0).factorCholesky()

        assertEquals(0, factor.size)
        assertEquals(0.0, factor.logDeterminant())
        assertTrue(factor.solve(doubleArrayOf()).isEmpty())
        assertEquals(3, factor.solve(DenseMatrix.zero(0, 3)).cols)
        assertEquals(0, DenseMatrix.diagonal(2, 4.0).factorCholesky().solve(DenseMatrix.zero(2, 0)).cols)
    }

    @Test
    fun `indefinite and singular matrices report their failing pivot without regularization`() {
        for (diagonal in listOf(0.0, -1.0)) {
            val matrix = DenseMatrix.diagonal(2, 1.0)
            matrix[1, 1] = diagonal

            val error = assertFailsWith<NotPositiveDefinite> { matrix.factorCholesky() }

            assertEquals(1, error.pivotIndex)
            assertEquals(diagonal, error.pivot)
        }
    }

    @Test
    fun `public factorization and solves reject invalid shapes and nonfinite input`() {
        assertFailsWith<IllegalArgumentException> { DenseMatrix.zero(2, 3).factorCholesky() }
        for (value in listOf(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            val matrix = DenseMatrix.diagonal(2, 1.0)
            matrix[1, 0] = value
            assertFailsWith<IllegalArgumentException> { matrix.factorCholesky() }
        }
        val factor = DenseMatrix.diagonal(2, 1.0).factorCholesky()
        assertFailsWith<IllegalArgumentException> { factor.solve(doubleArrayOf(1.0)) }
        assertFailsWith<IllegalArgumentException> { factor.solve(DenseMatrix.zero(3, 1)) }
        assertFailsWith<IllegalArgumentException> { factor.solveLower(DenseMatrix.zero(3, 1)) }
    }
}

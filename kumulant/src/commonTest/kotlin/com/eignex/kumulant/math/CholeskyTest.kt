package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.dense.cholesky
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CholeskyTest {

    // Reconstruct A = L * LT from the lower triangle of a factor.
    private fun product(l: DenseMatrix): Array<DoubleArray> {
        val n = l.rows
        return Array(n) { i ->
            DoubleArray(n) { j ->
                var s = 0.0
                for (k in 0..minOf(i, j)) s += l[i, k] * l[j, k]
                s
            }
        }
    }

    private fun assertMatrixEquals(expected: Array<DoubleArray>, actual: Array<DoubleArray>, tol: Double = 1e-9) {
        for (i in expected.indices) {
            for (j in expected[i].indices) {
                assertEquals(expected[i][j], actual[i][j], tol, "entry ($i, $j)")
            }
        }
    }

    @Test
    fun `downdate subtracts the rank-one term from the factored matrix`() {
        val a = arrayOf(
            doubleArrayOf(4.0, 1.0),
            doubleArrayOf(1.0, 3.0),
        )
        val x = doubleArrayOf(0.5, 0.25)
        val l = DenseMatrix.of(a).cholesky().l

        val norm = l.choleskyDowndateInPlace(DenseVector.of(x))

        assertEquals(0.0, norm, "downdate inside the cone reports success")
        val expected = Array(2) { i -> DoubleArray(2) { j -> a[i][j] - x[i] * x[j] } }
        assertMatrixEquals(expected, product(l))
    }

    @Test
    fun `downdate handles a three by three factor`() {
        val a = arrayOf(
            doubleArrayOf(6.0, 1.0, 0.5),
            doubleArrayOf(1.0, 5.0, 2.0),
            doubleArrayOf(0.5, 2.0, 4.0),
        )
        val x = doubleArrayOf(0.3, -0.6, 0.2)
        val l = DenseMatrix.of(a).cholesky().l

        assertEquals(0.0, l.choleskyDowndateInPlace(DenseVector.of(x)))

        val expected = Array(3) { i -> DoubleArray(3) { j -> a[i][j] - x[i] * x[j] } }
        assertMatrixEquals(expected, product(l))
    }

    @Test
    fun `downdate reports a norm at or above one when it would leave the cone`() {
        val l = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 0.0),
                doubleArrayOf(0.0, 1.0),
            ),
        ).cholesky().l

        // x with ||L^-1 x|| >= 1 cannot be subtracted and stay positive-definite.
        val norm = l.choleskyDowndateInPlace(DenseVector.of(doubleArrayOf(3.0, 4.0)))

        assertTrue(norm >= 1.0, "expected a rejected downdate, got norm=$norm")
    }

    @Test
    fun `zeroUpperTriangle clears everything above the diagonal`() {
        val m = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 2.0, 3.0),
                doubleArrayOf(4.0, 5.0, 6.0),
                doubleArrayOf(7.0, 8.0, 9.0),
            ),
        )

        m.zeroUpperTriangle()

        for (i in 0 until 3) {
            for (j in 0 until 3) {
                if (j > i) assertEquals(0.0, m[i, j], "entry ($i, $j) above the diagonal")
            }
        }
        assertEquals(4.0, m[1, 0])
        assertEquals(9.0, m[2, 2])
    }
}

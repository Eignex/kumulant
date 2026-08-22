@file:OptIn(com.eignex.koblas.UnsafeKoblasApi::class)

package com.eignex.kumulant.math

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.dense.cholesky
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CholeskyTest {

    // Reconstruct A = L * LT from the lower triangle of a factor.
    private fun product(l: F64DenseMatrix): Array<DoubleArray> {
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
        val l = F64DenseMatrix.of(a).cholesky().l

        val norm = l.choleskyDowndateInPlace(F64DenseVector.of(x))

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
        val l = F64DenseMatrix.of(a).cholesky().l

        assertEquals(0.0, l.choleskyDowndateInPlace(F64DenseVector.of(x)))

        val expected = Array(3) { i -> DoubleArray(3) { j -> a[i][j] - x[i] * x[j] } }
        assertMatrixEquals(expected, product(l))
    }

    @Test
    fun `downdate reports a norm at or above one when it would leave the cone`() {
        val l = F64DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 0.0),
                doubleArrayOf(0.0, 1.0),
            ),
        ).cholesky().l

        // x with ||L^-1 x|| >= 1 cannot be subtracted and stay positive-definite.
        val norm = l.choleskyDowndateInPlace(F64DenseVector.of(doubleArrayOf(3.0, 4.0)))

        assertTrue(norm >= 1.0, "expected a rejected downdate, got norm=$norm")
    }

    @Test
    fun `downdate reports the norm at the cone boundary rather than silently doing nothing`() {
        // ||L^-1 x|| == 1 exactly: the downdate cannot proceed, and the factor is left alone.
        val l = F64DenseMatrix.of(
            arrayOf(
                doubleArrayOf(4.0, 0.0),
                doubleArrayOf(0.0, 4.0),
            ),
        ).cholesky().l
        val before = product(l)

        val norm = l.choleskyDowndateInPlace(F64DenseVector.of(doubleArrayOf(2.0, 0.0)))

        assertEquals(1.0, norm, 1e-12, "the boundary reports rejection, not success")
        assertMatrixEquals(before, product(l))
    }

    @Test
    fun `downdate takes a sparse vector`() {
        val a = arrayOf(
            doubleArrayOf(5.0, 1.0, 0.0),
            doubleArrayOf(1.0, 4.0, 1.0),
            doubleArrayOf(0.0, 1.0, 3.0),
        )
        val l = F64DenseMatrix.of(a).cholesky().l
        val x = doubleArrayOf(0.0, 0.4, 0.0)

        val norm = l.choleskyDowndateInPlace(
            F64SparseVector.of(size = 3, indices = intArrayOf(1), values = doubleArrayOf(0.4)),
        )

        assertEquals(0.0, norm)
        val expected = Array(3) { i -> DoubleArray(3) { j -> a[i][j] - x[i] * x[j] } }
        assertMatrixEquals(expected, product(l))
    }

    @Test
    fun `downdate handles a one by one factor`() {
        val l = F64DenseMatrix.of(arrayOf(doubleArrayOf(4.0))).cholesky().l

        assertEquals(0.0, l.choleskyDowndateInPlace(F64DenseVector.of(doubleArrayOf(1.0))))

        assertEquals(3.0, product(l)[0][0], 1e-12)
    }

    @Test
    fun `downdate of a zero vector leaves the factor unchanged`() {
        val l = F64DenseMatrix.of(
            arrayOf(
                doubleArrayOf(2.0, 0.5),
                doubleArrayOf(0.5, 1.0),
            ),
        ).cholesky().l
        val before = product(l)

        assertEquals(0.0, l.choleskyDowndateInPlace(F64DenseVector.of(doubleArrayOf(0.0, 0.0))))

        assertMatrixEquals(before, product(l))
    }

    @Test
    fun `zeroUpperTriangle clears everything above the diagonal`() {
        val m = F64DenseMatrix.of(
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

    @Test
    fun `downdate through a singular factor leaves the factor finite`() {
        val l = F64DenseMatrix.of(
            arrayOf(
                doubleArrayOf(0.0, 0.0),
                doubleArrayOf(0.0, 1.0),
            ),
        )
        l.choleskyDowndateInPlace(F64DenseVector.of(doubleArrayOf(1.0, 0.0)))
        for (i in 0 until 2) {
            for (j in 0 until 2) assertTrue(l[i, j].isFinite(), "entry ($i, $j) = ${l[i, j]}")
        }
    }
}

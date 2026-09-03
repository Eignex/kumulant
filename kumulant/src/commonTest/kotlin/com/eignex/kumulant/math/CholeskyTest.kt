@file:OptIn(com.eignex.koblas.UnsafeKoblasApi::class)

package com.eignex.kumulant.math

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.dense.cholesky
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CholeskyTest {

    private class StridedVector(private val backing: DoubleArray) : F64VectorLike {
        override val size: Int get() = backing.size / 2
        override fun get(i: Int): Double = backing[i * 2]
        override fun toDoubleArray(): DoubleArray = DoubleArray(size) { this[it] }
    }

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

    private fun updated(a: Array<DoubleArray>, x: DoubleArray, sigma: Double = 1.0): Array<DoubleArray> =
        Array(a.size) { i -> DoubleArray(a.size) { j -> a[i][j] + sigma * x[i] * x[j] } }

    private fun assertMatrixEquals(expected: Array<DoubleArray>, actual: Array<DoubleArray>, tol: Double = 1e-9) {
        for (i in expected.indices) {
            for (j in expected[i].indices) {
                assertEquals(expected[i][j], actual[i][j], tol, "entry ($i, $j)")
            }
        }
    }

    @Test
    fun `update adds the rank-one term to the factored matrix`() {
        val cases = listOf(
            arrayOf(doubleArrayOf(4.0)) to doubleArrayOf(1.0),
            arrayOf(doubleArrayOf(4.0, 1.0), doubleArrayOf(1.0, 3.0)) to doubleArrayOf(0.5, 0.25),
            arrayOf(doubleArrayOf(4.0, 1.0), doubleArrayOf(1.0, 3.0)) to doubleArrayOf(-3.0, 7.0),
            arrayOf(
                doubleArrayOf(6.0, 1.0, 0.5),
                doubleArrayOf(1.0, 5.0, 2.0),
                doubleArrayOf(0.5, 2.0, 4.0),
            ) to doubleArrayOf(0.3, -0.6, 0.2),
            arrayOf(
                doubleArrayOf(2.0, 0.0, 0.0, 0.0),
                doubleArrayOf(0.0, 2.0, 0.0, 0.0),
                doubleArrayOf(0.0, 0.0, 2.0, 0.0),
                doubleArrayOf(0.0, 0.0, 0.0, 2.0),
            ) to doubleArrayOf(1.0, -2.0, 3.0, -4.0),
        )

        for ((a, x) in cases) {
            val l = F64DenseMatrix.of(a).cholesky().l

            l.choleskyUpdateInPlace(F64DenseVector.of(x))

            assertMatrixEquals(updated(a, x), product(l))
        }
    }

    @Test
    fun `update keeps the factor diagonal positive`() {
        val l = F64DenseMatrix.of(arrayOf(doubleArrayOf(1.0, 0.0), doubleArrayOf(0.0, 1.0))).cholesky().l

        l.choleskyUpdateInPlace(F64DenseVector.of(doubleArrayOf(-40.0, -30.0)))

        for (i in 0 until 2) assertTrue(l[i, i] > 0.0, "diagonal entry ($i, $i) = ${l[i, i]}")
    }

    @Test
    fun `sigma scales the rank-one term`() {
        val a = arrayOf(doubleArrayOf(4.0, 1.0), doubleArrayOf(1.0, 3.0))
        val x = doubleArrayOf(0.5, 0.25)
        val l = F64DenseMatrix.of(a).cholesky().l

        l.choleskyUpdateInPlace(F64DenseVector.of(x), sigma = 9.0)

        assertMatrixEquals(updated(a, x, sigma = 9.0), product(l))
    }

    @Test
    fun `a zero sigma leaves the factor unchanged`() {
        val l = F64DenseMatrix.of(arrayOf(doubleArrayOf(4.0, 1.0), doubleArrayOf(1.0, 3.0))).cholesky().l
        val before = product(l)

        l.choleskyUpdateInPlace(F64DenseVector.of(doubleArrayOf(2.0, -5.0)), sigma = 0.0)

        assertMatrixEquals(before, product(l))
    }

    @Test
    fun `a negative sigma is rejected as a downdate`() {
        val l = F64DenseMatrix.of(arrayOf(doubleArrayOf(4.0, 1.0), doubleArrayOf(1.0, 3.0))).cholesky().l

        assertFailsWith<IllegalArgumentException> {
            l.choleskyUpdateInPlace(F64DenseVector.of(doubleArrayOf(0.5, 0.25)), sigma = -1.0)
        }
    }

    @Test
    fun `update leaves the source vector untouched`() {
        val l = F64DenseMatrix.of(arrayOf(doubleArrayOf(4.0, 1.0), doubleArrayOf(1.0, 3.0))).cholesky().l
        val x = F64DenseVector.of(doubleArrayOf(0.5, 0.25))

        l.choleskyUpdateInPlace(x, sigma = 4.0)

        assertEquals(0.5, x[0], 0.0)
        assertEquals(0.25, x[1], 0.0)
    }

    @Test
    fun `update absorbs a near-singular direction the factor barely spans`() {
        val a = arrayOf(doubleArrayOf(1e-14, 0.0), doubleArrayOf(0.0, 1.0))
        val x = doubleArrayOf(1.0, 1.0)
        val l = F64DenseMatrix.of(a).cholesky().l

        l.choleskyUpdateInPlace(F64DenseVector.of(x))

        assertMatrixEquals(updated(a, x), product(l), tol = 1e-12)
    }

    @Test
    fun `update through a singular factor restores the rank it was missing`() {
        val l = F64DenseMatrix.of(arrayOf(doubleArrayOf(0.0, 0.0), doubleArrayOf(0.0, 1.0)))

        l.choleskyUpdateInPlace(F64DenseVector.of(doubleArrayOf(1.0, 0.0)))

        assertMatrixEquals(arrayOf(doubleArrayOf(1.0, 0.0), doubleArrayOf(0.0, 1.0)), product(l))
    }

    @Test
    fun `workspace update reuses nested scratch`() {
        val a = arrayOf(doubleArrayOf(4.0, 1.0), doubleArrayOf(1.0, 3.0))
        val l = F64DenseMatrix.of(a).cholesky().l
        val workspace = Workspace().apply { reserve(2, 2) }
        val x = doubleArrayOf(0.5, 0.25)
        val z = doubleArrayOf(0.1, 0.1)

        l.choleskyUpdateInPlace(F64DenseVector.of(x), workspace = workspace)
        l.choleskyUpdateInPlace(F64DenseVector.of(z), workspace = workspace)

        assertMatrixEquals(updated(updated(a, x), z), product(l))
    }

    @Test
    fun `update takes a sparse vector`() {
        val a = arrayOf(
            doubleArrayOf(5.0, 1.0, 0.0),
            doubleArrayOf(1.0, 4.0, 1.0),
            doubleArrayOf(0.0, 1.0, 3.0),
        )
        val l = F64DenseMatrix.of(a).cholesky().l

        l.choleskyUpdateInPlace(F64SparseVector.of(size = 3, indices = intArrayOf(1), values = doubleArrayOf(0.4)))

        assertMatrixEquals(updated(a, doubleArrayOf(0.0, 0.4, 0.0)), product(l))
    }

    @Test
    fun `update takes a generic strided vector`() {
        val a = arrayOf(doubleArrayOf(4.0, 1.0), doubleArrayOf(1.0, 3.0))
        val l = F64DenseMatrix.of(a).cholesky().l

        l.choleskyUpdateInPlace(StridedVector(doubleArrayOf(0.5, 9.0, 0.25, 9.0)))

        assertMatrixEquals(updated(a, doubleArrayOf(0.5, 0.25)), product(l))
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
}

package com.eignex.kumulant.math

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseMatrix
import kotlin.math.abs
import kotlin.math.min
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CholeskyTest {
    @Test
    fun `factorization recovers a known factor across block boundaries`() {
        for (n in listOf(0, 1, 63, 64, 65, 129, 193, 321)) {
            val tail = 1.0 / (n + 1)
            val a = F64DenseMatrix.wrap(
                n, n,
                DoubleArray(n * n) { index ->
                val i = index % n
                val j = index / n
                if (i < j) Double.NaN else min(i, j) * tail * tail + if (i == j) 4.0 else 2.0 * tail
            }
            )
            val original = a.data.copyOf()

            val factor = a.cholesky()

            var maxError = 0.0
            for (j in 0 until n) {
                for (i in 0 until n) {
                    val expected = if (i < j) 0.0 else if (i == j) {
                        2.0
                    } else {
                        tail
                    }
                    maxError = maxOf(maxError, abs(expected - factor.l[i, j]))
                }
            }
            assertTrue(maxError < 1e-12, "n=$n factor error=$maxError")
            assertContentEquals(original, a.data)
        }
    }

    @Test
    fun `strict factorization rejects the first non-positive pivot`() {
        for (diagonal in listOf(0.0, -1.0)) {
            val a = F64DenseMatrix.wrap(2, 2, doubleArrayOf(1.0, 0.0, 0.0, diagonal))

            val error = assertFailsWith<NotPositiveDefinite> { a.cholesky() }

            assertEquals(1, error.pivotIndex)
            assertEquals(diagonal, error.pivot)
        }
    }

    @Test
    fun `every policy rejects NaN pivots`() {
        for (policy in listOf(CholeskyPolicy.Strict, CholeskyPolicy.Regularize())) {
            val a = F64DenseMatrix.wrap(1, 1, doubleArrayOf(Double.NaN))

            assertFailsWith<NotPositiveDefinite> { a.cholesky(policy) }
        }
    }

    @Test
    fun `regularization bounds the column below a small pivot`() {
        for (pivot in listOf(-1.0, 0.0, 1e-12)) {
            val a = F64DenseMatrix.wrap(2, 2, doubleArrayOf(pivot, 3.0, 3.0, 2.0))

            val factor = a.cholesky(CholeskyPolicy.Regularize())

            assertContentEquals(doubleArrayOf(3.0, 1.0, 0.0, 1.0), factor.l.data)
            assertEquals(1, factor.regularizations)
        }
    }

    @Test
    fun `regularization floors a small uncoupled pivot`() {
        val a = F64DenseMatrix.wrap(1, 1, doubleArrayOf(1e-12))

        val factor = a.cholesky(CholeskyPolicy.Regularize())

        assertEquals(sqrt(1e-10), factor.l[0, 0])
    }

    @Test
    fun `rank one update represents the weighted outer product without changing its input`() {
        for (workspace in listOf(null, Workspace().apply { reserve(3, 2) })) {
            val factor = F64DenseMatrix.diagonal(3, 2.0).cholesky()
            val v = doubleArrayOf(1.0, -2.0, 3.0)

            factor.rankUpdate(v, 4.0, workspace)

            for (i in 0 until 3) {
                for (j in 0 until 3) {
                    var actual = 0.0
                    for (k in 0 until 3) actual += factor.l[i, k] * factor.l[j, k]
                    val expected = (if (i == j) 2.0 else 0.0) + 4.0 * v[i] * v[j]
                    assertEquals(expected, actual, 1e-12)
                }
            }
            assertContentEquals(doubleArrayOf(1.0, -2.0, 3.0), v)
        }
    }

    @Test
    fun `zero rank update leaves the factor untouched`() {
        val factor = F64DenseMatrix.diagonal(2, 2.0).cholesky()
        val original = factor.l.data.copyOf()

        factor.rankUpdate(doubleArrayOf(Double.NaN, Double.NaN), 0.0)

        assertContentEquals(original, factor.l.data)
    }

    @Test
    fun `rank update avoids squaring overflow and underflow`() {
        for (magnitude in listOf(1e-200, 1e200)) {
            val factor = CholeskyFactor(F64DenseMatrix.wrap(1, 1, doubleArrayOf(magnitude)))

            factor.rankUpdate(doubleArrayOf(-magnitude), 1.0)

            assertTrue(factor.l[0, 0].isFinite())
            assertEquals(sqrt(2.0), factor.l[0, 0] / magnitude, 1e-14)
        }
    }

    @Test
    fun `solve allows the destination to be the right hand side`() {
        val factor = F64DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).cholesky()
        val b = doubleArrayOf(8.0, 12.0)

        factor.solveInto(b, b)

        assertContentEquals(doubleArrayOf(1.0, 2.0), b)
    }

    @Test
    fun `inverse overwrites both triangles and preserves the factor with reused scratch`() {
        for (workspace in listOf(null, Workspace().apply { reserve(2, 1) })) {
            val factor = F64DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).cholesky()
            val original = factor.l.data.copyOf()
            val out = F64DenseMatrix.wrap(2, 2, DoubleArray(4) { Double.NaN })

            repeat(2) { factor.invertInto(out, workspace) }

            assertContentEquals(doubleArrayOf(0.3125, -0.125, -0.125, 0.25), out.data)
            assertContentEquals(original, factor.l.data)
        }
    }

    @Test
    fun `inverse rejects a destination sharing factor storage`() {
        val factor = F64DenseMatrix.diagonal(2, 1.0).cholesky()
        val alias = F64DenseMatrix.wrap(2, 2, factor.l.data)

        assertFailsWith<IllegalArgumentException> { factor.invertInto(alias) }

        assertContentEquals(doubleArrayOf(1.0, 0.0, 0.0, 1.0), factor.l.data)
    }
}

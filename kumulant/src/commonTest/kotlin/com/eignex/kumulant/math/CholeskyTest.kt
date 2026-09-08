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
import kotlin.test.assertSame
import kotlin.test.assertTrue

class CholeskyTest {
    @Test
    fun `factorization recovers a known factor across block boundaries`() {
        for (n in listOf(0, 1, 63, 64, 65, 129, 193, 321)) {
            val tail = 1.0 / (n + 1)
            val a = F64DenseMatrix.wrap(
                n,
                n,
                DoubleArray(n * n) { index ->
                    val i = index % n
                    val j = index / n
                    if (i < j) Double.NaN else min(i, j) * tail * tail + if (i == j) 4.0 else 2.0 * tail
                },
            )
            val original = a.data.copyOf()

            val factor = a.cholesky()

            var maxError = 0.0
            for (j in 0 until n) {
                for (i in 0 until n) {
                    val expected = if (i < j) {
                        0.0
                    } else if (i == j) {
                        2.0
                    } else {
                        tail
                    }
                    maxError = maxOf(maxError, abs(expected - factor[i, j]))
                }
            }
            assertTrue(maxError < 1e-12, "n=$n factor error=$maxError")
            assertContentEquals(original, a.data)
        }
    }

    @Test
    fun `factorization overwrites a reused destination without changing its input`() {
        val a = F64DenseMatrix.diagonal(65, 4.0)
        a[64, 0] = 2.0
        a[64, 64] = 5.0
        val original = a.data.copyOf()
        val out = F64DenseMatrix.wrap(65, 65, DoubleArray(65 * 65) { Double.NaN })

        repeat(2) {
            val factor = a.choleskyInto(out)

            assertSame(out, factor)
            for (j in 0 until 65) {
                for (i in 0 until 65) {
                    val expected = if (i == j) 2.0 else if (i == 64 && j == 0) {
                        1.0
                    } else {
                        0.0
                    }
                    assertEquals(expected, out[i, j])
                }
            }
            assertContentEquals(original, a.data)
        }
    }

    @Test
    fun `factorization allows the destination to share input storage`() {
        for (sameMatrix in listOf(true, false)) {
            val a = F64DenseMatrix.diagonal(65, 4.0)
            a[64, 0] = 2.0
            a[64, 64] = 5.0
            for (j in 0 until 65) for (i in 0 until j) a[i, j] = Double.NaN
            val out = if (sameMatrix) a else F64DenseMatrix.wrap(65, 65, a.data)

            val factor = a.choleskyInto(out)

            assertSame(out, factor)
            for (j in 0 until 65) {
                for (i in 0 until 65) {
                    val expected = if (i == j) 2.0 else if (i == 64 && j == 0) {
                        1.0
                    } else {
                        0.0
                    }
                    assertEquals(expected, factor[i, j])
                }
            }
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

            assertContentEquals(doubleArrayOf(3.0, 1.0, 0.0, 1.0), factor.data)
        }
    }

    @Test
    fun `regularization floors a small uncoupled pivot`() {
        val a = F64DenseMatrix.wrap(1, 1, doubleArrayOf(1e-12))

        val factor = a.cholesky(CholeskyPolicy.Regularize())

        assertEquals(sqrt(1e-10), factor[0, 0])
    }

    @Test
    fun `rank one update represents the weighted outer product without changing its input`() {
        for (workspace in listOf(null, Workspace().apply { reserve(3, 2) })) {
            val factor = F64DenseMatrix.diagonal(3, 2.0).cholesky()
            val v = doubleArrayOf(1.0, -2.0, 3.0)

            factor.choleskyRankUpdate(v, 4.0, workspace)

            for (i in 0 until 3) {
                for (j in 0 until 3) {
                    var actual = 0.0
                    for (k in 0 until 3) actual += factor[i, k] * factor[j, k]
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
        val original = factor.data.copyOf()

        factor.choleskyRankUpdate(doubleArrayOf(Double.NaN, Double.NaN), 0.0)

        assertContentEquals(original, factor.data)
    }

    @Test
    fun `rank update avoids squaring overflow and underflow`() {
        for (magnitude in listOf(1e-200, 1e200)) {
            val factor = F64DenseMatrix.wrap(1, 1, doubleArrayOf(magnitude))

            factor.choleskyRankUpdate(doubleArrayOf(-magnitude), 1.0)

            assertTrue(factor[0, 0].isFinite())
            assertEquals(sqrt(2.0), factor[0, 0] / magnitude, 1e-14)
        }
    }

    @Test
    fun `solve allows the destination to be the right hand side`() {
        val factor = F64DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).cholesky()
        val b = doubleArrayOf(8.0, 12.0)

        factor.choleskySolveInto(b, b)

        assertContentEquals(doubleArrayOf(1.0, 2.0), b)
    }

    @Test
    fun `solve into a separate destination preserves the right hand side`() {
        val factor = F64DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).cholesky()
        val b = doubleArrayOf(8.0, 12.0)
        val out = doubleArrayOf(Double.NaN, Double.NaN)

        val solution = factor.choleskySolveInto(b, out)

        assertSame(out, solution)
        assertContentEquals(doubleArrayOf(1.0, 2.0), out)
        assertContentEquals(doubleArrayOf(8.0, 12.0), b)
    }

    @Test
    fun `inverse overwrites both triangles and preserves the factor with reused scratch`() {
        for (workspace in listOf(null, Workspace().apply { reserve(2, 1) })) {
            val factor = F64DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).cholesky()
            val original = factor.data.copyOf()
            val out = F64DenseMatrix.wrap(2, 2, DoubleArray(4) { Double.NaN })

            repeat(2) { factor.choleskyInvertInto(out, workspace) }

            assertContentEquals(doubleArrayOf(0.3125, -0.125, -0.125, 0.25), out.data)
            assertContentEquals(original, factor.data)
        }
    }

    @Test
    fun `inverse rejects a destination sharing factor storage`() {
        val factor = F64DenseMatrix.diagonal(2, 1.0).cholesky()
        val alias = F64DenseMatrix.wrap(2, 2, factor.data)

        assertFailsWith<IllegalArgumentException> { factor.choleskyInvertInto(alias) }

        assertContentEquals(doubleArrayOf(1.0, 0.0, 0.0, 1.0), factor.data)
    }
}

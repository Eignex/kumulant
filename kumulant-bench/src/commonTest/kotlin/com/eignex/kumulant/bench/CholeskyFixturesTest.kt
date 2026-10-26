package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.kumulant.math.NotPositiveDefinite
import com.eignex.kumulant.math.factorCholesky
import kotlin.math.abs
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CholeskyFixturesTest {
    @Test
    fun `seeded matrices and right hand sides are reproducible`() {
        for (kind in listOf("precision", "rbf", "matern")) {
            assertContentEquals(CholeskyFixtures.matrix("$kind:32").values, CholeskyFixtures.matrix("$kind:32").values)
        }
        assertContentEquals(CholeskyFixtures.right(32, 16).values, CholeskyFixtures.right(32, 16).values)
    }

    @Test
    fun `covariance diagonal includes the specified noise and jitter`() {
        for (kind in listOf("rbf", "matern")) {
            val matrix = CholeskyFixtures.covariance(CholeskyFixtures.points(32), kind)

            for (i in 0 until matrix.rows) {
                assertEquals(1.0 + CholeskyFixtures.NOISE_VARIANCE + CholeskyFixtures.JITTER, matrix[i, i], 1e-15)
            }
        }
    }

    @Test
    fun `reusable factorization reconstructs benchmark matrices across block boundaries`() {
        val workspace = Workspace()
        for (size in listOf(63, 64, 65, 127, 128, 129)) {
            for (kind in listOf("precision", "rbf", "matern")) {
                val matrix = CholeskyFixtures.matrix("$kind:$size")

                val lower = matrix.factorCholesky(workspace).lowerFactor()

                var error = 0.0
                var scale = 0.0
                for (j in 0 until size) {
                    for (i in j until size) {
                        var actual = 0.0
                        for (k in 0..j) actual += lower[i, k] * lower[j, k]
                        error = maxOf(error, abs(actual - matrix[i, j]))
                        scale = maxOf(scale, abs(matrix[i, j]))
                    }
                }
                assertTrue(error / scale < 1e-13, "$kind:$size relative reconstruction error=${error / scale}")
            }
        }
    }

    @Test
    fun `rank updates agree with full factorization across scales`() {
        val workspace = Workspace()
        for (size in listOf(8, 32, 65)) {
            for (scale in listOf(1e-100, 1.0, 1e100)) {
                val matrix = CholeskyFixtures.precision(size)
                for (i in matrix.values.indices) matrix.values[i] *= scale
                var factor = matrix.factorCholesky(workspace)
                val updates = CholeskyFixtures.right(size, 4)

                for (j in 0 until updates.cols) {
                    val update = DoubleArray(size) { updates[it, j] * sqrt(scale) }
                    factor = factor.rankUpdated(update, workspace)
                    CholeskyFixtures.addOuterProduct(matrix, update)
                }

                val expected = matrix.factorCholesky().lowerFactor().values
                val actual = factor.lowerFactor().values
                var error = 0.0
                var magnitude = 0.0
                for (i in expected.indices) {
                    error = maxOf(error, abs(actual[i] - expected[i]))
                    magnitude = maxOf(magnitude, abs(expected[i]))
                }
                assertTrue(error / magnitude < 1e-12, "size=$size scale=$scale relative error=${error / magnitude}")
            }
        }
    }

    @Test
    fun `covariance extensions agree across scales and block boundaries`() {
        val workspace = Workspace()
        for (kind in listOf("rbf", "matern")) {
            for (size in listOf(63, 65)) {
                for (scale in listOf(1e-100, 1.0, 1e100)) {
                    val fixture = CholeskyFixtures.extension(size, 4, kind, scale)
                    val factor = fixture.base.factorCholesky(workspace)

                    val actual = factor.extended(fixture.cross, fixture.diagonal, workspace).lowerFactor().values

                    val expected = fixture.full.factorCholesky().lowerFactor().values
                    var error = 0.0
                    var magnitude = 0.0
                    for (i in expected.indices) {
                        error = maxOf(error, abs(actual[i] - expected[i]))
                        magnitude = maxOf(magnitude, abs(expected[i]))
                    }
                    assertTrue(error / magnitude < 1e-8, "$kind:$size scale=$scale relative error=${error / magnitude}")
                }
            }
        }
    }

    @Test
    fun `matrix solves have small backward error on clustered covariance fixtures`() {
        for (kind in listOf("precision", "rbf", "matern")) {
            val matrix = CholeskyFixtures.matrix("$kind:32")
            val right = CholeskyFixtures.right(32, 3)

            val solution = matrix.factorCholesky().solve(right)

            var matrixNorm = 0.0
            for (i in 0 until matrix.rows) {
                var rowSum = 0.0
                for (j in 0 until matrix.cols) rowSum += abs(matrix[i, j])
                matrixNorm = maxOf(matrixNorm, rowSum)
            }
            for (column in 0 until right.cols) {
                var residual = 0.0
                var solutionNorm = 0.0
                var rightNorm = 0.0
                for (i in 0 until matrix.rows) {
                    var actual = 0.0
                    for (j in 0 until matrix.cols) actual += matrix[i, j] * solution[j, column]
                    residual = maxOf(residual, abs(actual - right[i, column]))
                    solutionNorm = maxOf(solutionNorm, abs(solution[i, column]))
                    rightNorm = maxOf(rightNorm, abs(right[i, column]))
                }
                val error = residual / (matrixNorm * solutionNorm + rightNorm)
                assertTrue(error < 1e-13, "$kind column=$column backward error=$error")
            }
        }
    }

    @Test
    fun `scaled diagonal matrices have the analytical log determinant`() {
        for (scale in listOf(1e-100, 1.0, 1e100)) {
            val matrix = DenseMatrix.diagonal(65, scale)

            val actual = matrix.factorCholesky().logDeterminant()

            assertEquals(65 * ln(scale), actual, 1e-10)
        }
    }

    @Test
    fun `unshifted duplicate inputs are rejected`() {
        for (kind in listOf("rbf", "matern")) {
            val points = DoubleArray(3)
            val singular = CholeskyFixtures.covariance(points, kind, noiseVariance = 0.0, jitter = 0.0)

            assertFailsWith<NotPositiveDefinite> { singular.factorCholesky() }
        }
    }

    @Test
    fun `explicit diagonal shift stabilizes duplicate inputs`() {
        for (kind in listOf("rbf", "matern")) {
            val shifted = CholeskyFixtures.covariance(DoubleArray(3), kind, noiseVariance = 0.0, jitter = 1e-10)

            assertTrue(shifted.factorCholesky().logDeterminant().isFinite())
        }
    }
}

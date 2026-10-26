package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.sqrt
import kotlin.random.Random

internal object CholeskyFixtures {
    const val SEED = 139
    const val NOISE_VARIANCE = 1e-6
    const val JITTER = 1e-10
    const val FITTING_ROUND_SIZE = 25

    fun matrix(matrixCase: String): DenseMatrix {
        val (kind, order) = matrixCase.split(':')
        val size = order.toInt()
        return when (kind) {
            "precision" -> precision(size)
            "rbf", "matern" -> covariance(points(size), kind)
            else -> error("Unknown Cholesky fixture: $matrixCase")
        }
    }

    fun precision(size: Int): DenseMatrix {
        val random = Random(SEED)
        val matrix = DenseMatrix.diagonal(size, 1.0)
        val observation = DoubleArray(size)
        repeat(2 * size) {
            for (i in observation.indices) observation[i] = random.nextDouble(-1.0, 1.0) / sqrt(size.toDouble())
            for (j in 0 until size) {
                for (i in j until size) {
                    val value = matrix[i, j] + observation[i] * observation[j]
                    matrix[i, j] = value
                    matrix[j, i] = value
                }
            }
        }
        return matrix
    }

    fun points(size: Int): DoubleArray {
        val random = Random(SEED)
        // Paired inputs stress near-dependence without making the prescribed shift size-dependent.
        return DoubleArray(size) { (it / 2).toDouble() / size + random.nextDouble() * 1e-8 }
    }

    fun covariance(
        points: DoubleArray,
        kind: String = "rbf",
        lengthScale: Double = 0.15,
        noiseVariance: Double = NOISE_VARIANCE,
        jitter: Double = JITTER,
    ): DenseMatrix {
        require(kind == "rbf" || kind == "matern") { "Unknown covariance fixture: $kind" }
        val matrix = DenseMatrix.zero(points.size, points.size)
        for (j in points.indices) {
            for (i in j until points.size) {
                val distance = abs(points[i] - points[j]) / lengthScale
                val value = if (kind == "rbf") {
                    exp(-0.5 * distance * distance)
                } else {
                    val scaled = sqrt(5.0) * distance
                    (1.0 + scaled + scaled * scaled / 3.0) * exp(-scaled)
                }
                matrix[i, j] = value
                matrix[j, i] = value
            }
            matrix[j, j] += noiseVariance + jitter
        }
        return matrix
    }

    fun right(size: Int, columns: Int = 1): DenseMatrix {
        val random = Random(SEED + 1)
        return DenseMatrix.wrap(size, columns, DoubleArray(size * columns) { random.nextDouble(-1.0, 1.0) })
    }

    fun addOuterProduct(matrix: DenseMatrix, update: DoubleArray) {
        for (j in 0 until matrix.cols) {
            for (i in j until matrix.rows) {
                val value = matrix[i, j] + update[i] * update[j]
                matrix[i, j] = value
                matrix[j, i] = value
            }
        }
    }

    fun extension(size: Int, added: Int, kind: String = "rbf", scale: Double = 1.0): CholeskyExtensionFixture {
        val full = covariance(points(size + added), kind)
        for (i in full.values.indices) full.values[i] *= scale
        val base = DenseMatrix.zero(size, size)
        val cross = DenseMatrix.zero(size, added)
        val diagonal = DenseMatrix.zero(added, added)
        for (j in 0 until size) for (i in 0 until size) base[i, j] = full[i, j]
        for (j in 0 until added) {
            for (i in 0 until size) cross[i, j] = full[i, size + j]
            for (i in 0 until added) diagonal[i, j] = full[size + i, size + j]
        }
        return CholeskyExtensionFixture(base, cross, diagonal, full)
    }
}

internal data class CholeskyExtensionFixture(
    val base: DenseMatrix,
    val cross: DenseMatrix,
    val diagonal: DenseMatrix,
    val full: DenseMatrix,
)

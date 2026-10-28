package com.eignex.kumulant.stat.summary

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Vector
import com.eignex.koblas.koblas
import com.eignex.kumulant.math.CholeskyFactor

/** Common location estimate for observations with a supplied positive-definite covariance. */
data class CorrelatedMeanEstimate(
    /** Generalized least-squares mean; correlation can give observations negative coefficients. */
    val mean: Double,
    /** Variance of the mean conditional on the supplied covariance, without covariance-estimation uncertainty. */
    val varianceOfMean: Double,
)

/**
 * Estimate a common mean using the covariance represented by [covariance].
 * Computes `(1' C^-1 y) / (1' C^-1 1)` and variance `1 / (1' C^-1 1)` using solves,
 * without forming an inverse. Observations must be finite and nonempty. The covariance must
 * describe these observations, including observation noise, and have the same order.
 * Its scale is treated as known; estimated covariance uncertainty is not propagated.
 */
fun estimateCorrelatedMean(values: Vector, covariance: CholeskyFactor): CorrelatedMeanEstimate {
    require(values.size > 0 && values.size == covariance.size)
    val observations = values.toDoubleArray()
    require(observations.all { it.isFinite() })
    val anchor = estimateMean(values).mean
    val right = DenseMatrix.wrap(
        values.size,
        2,
        DoubleArray(2 * values.size) {
            if (it < values.size) 1.0 else observations[it - values.size] - anchor
        },
    )
    val whitened = covariance.solveLower(right).values
    val precision = koblas.vectorKernels.dot(whitened, 0, whitened, 0, values.size)
    require(precision > 0.0 && precision.isFinite()) { "Mean precision must be positive and finite" }
    val correction = koblas.vectorKernels.dot(whitened, 0, whitened, values.size, values.size) / precision
    val mean = anchor + correction
    val variance = 1.0 / precision
    require(mean.isFinite() && variance.isFinite()) { "Mean estimate is not finite; rescale observations" }
    return CorrelatedMeanEstimate(mean, variance)
}

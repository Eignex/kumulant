package com.eignex.kumulant.stat.regression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Regularisation knob shared across the regression stats that admit a closed-form
 * or proximal regularisation step. Concrete semantics depend on the host stat:
 *
 *  - [UnivariateRegressionStat]: selects the closed-form projection applied at
 *    `read()` time. The underlying Welford accumulator is independent of [Penalty],
 *    so switching penalties costs nothing in the hot update path.
 *  - [StochasticRegressionStat] and [DiagonalRegressionStat]: [L2] adds a
 *    `lambda * w_i` term to the gradient; [L1] takes a plain SGD step and then
 *    applies a per-coordinate proximal soft-thresholding sweep, which is what
 *    actually drives sparsity (subgradient L1 on its own does not).
 */
@Serializable
sealed interface Penalty {
    /** No regularisation: ordinary least squares or plain SGD. */
    @Serializable
    @SerialName("None")
    data object None : Penalty

    /**
     * L1 / Lasso. Univariate: soft-thresholds `sxy` by `lambda * totalWeights` before
     * computing the slope. SGD / Diagonal: proximal soft-threshold on each coordinate
     * after the gradient step.
     */
    @Serializable
    @SerialName("L1")
    data class L1(val lambda: Double) : Penalty

    /**
     * L2 / Ridge. Univariate: closed-form `slope = sxy / (sxx + lambda * totalWeights)`.
     * SGD / Diagonal: adds `lambda * w_i` to the per-coord gradient.
     */
    @Serializable
    @SerialName("L2")
    data class L2(val lambda: Double) : Penalty
}

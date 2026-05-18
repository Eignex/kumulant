package com.eignex.kumulant.stat.regression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Regularisation applied to a univariate least-squares fit. Selects the closed-form
 * projection used by [UnivariateRegressionStat] at `read()` time; the underlying
 * Chan-parallel Welford accumulator is the same for every penalty, so switching
 * penalties costs nothing in the hot update path.
 */
@Serializable
sealed interface Penalty {
    /** No regularisation: ordinary least squares. */
    @Serializable
    @SerialName("None")
    data object None : Penalty

    /** L1 / Lasso: soft-thresholded slope by `lambda * totalWeights`. */
    @Serializable
    @SerialName("L1")
    data class L1(val lambda: Double) : Penalty

    /** L2 / Ridge: shrinks the slope toward zero via `sxy / (sxx + lambda * totalWeights)`. */
    @Serializable
    @SerialName("L2")
    data class L2(val lambda: Double) : Penalty
}

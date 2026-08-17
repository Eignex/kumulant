package com.eignex.kumulant.stat.regression.glm

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Regularisation knob shared across regression stats. The mathematical effect is
 * always the same; the mechanics differ per host: a closed-form `read()`-time
 * projection in [UnivariateRegressionStat], lazy multiplicative scaling /
 * truncated-gradient in [StochasticRegressionStat], per-coordinate proximal step
 * (L1) or gradient term (L2) in [DiagonalRegressionStat]. See each stat's KDoc.
 */
@Serializable
sealed interface Penalty {
    /** No regularisation. */
    @Serializable
    @SerialName("None")
    data object None : Penalty

    /** L1 / Lasso: drives sparsity in the fitted weights. */
    @Serializable
    @SerialName("L1")
    data class L1(
        /** Regularisation strength. */
        val lambda: Double,
    ) : Penalty

    /** L2 / Ridge: shrinks the fitted weights toward zero. */
    @Serializable
    @SerialName("L2")
    data class L2(
        /** Regularisation strength. */
        val lambda: Double,
    ) : Penalty
}

// Soft-thresholding (the proximal operator of the L1 norm): shrink `w` toward zero by `threshold`,
// stopping at zero rather than crossing it. [Penalty.L1] does not require a positive `lambda`, so a
// negative threshold is reachable; without the non-positive guard the `when` reads inside out and
// pushes weights *away* from zero. Shrinking by a negative amount is not shrinking, so the penalty
// does nothing instead.
internal fun softThreshold(w: Double, threshold: Double): Double = when {
    threshold <= 0.0 -> w
    w > threshold -> w - threshold
    w < -threshold -> w + threshold
    else -> 0.0
}

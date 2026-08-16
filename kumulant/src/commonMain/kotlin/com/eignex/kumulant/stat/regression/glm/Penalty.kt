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

/**
 * Soft-thresholding (the proximal operator of the L1 norm): shrink [w] toward zero by [threshold],
 * stopping at zero rather than crossing it.
 *
 * This is the one operation every L1 path in the package needs, and all three hosts had their own copy:
 * [StochasticRegressionStat] as a named private function, [DiagonalRegressionStat] open-coded in its
 * proximal step, [UnivariateRegressionStat] open-coded in its `read`-time projection.
 *
 * The non-positive guard is not decoration, and it is why the three copies were not equivalent. A
 * [Penalty.L1] carries no positivity requirement on its `lambda`, so a negative one is constructible,
 * and every host derives its threshold by multiplying lambda by something positive. Without the guard
 * the `when` below reads inside out: at `w = 0` and `threshold = -t`, the first branch matches and
 * returns `+t`, so a negative lambda pushed weights *away* from zero instead of toward it. Shrinking by
 * a negative amount is not shrinking, so the guard returns [w] untouched and the penalty simply does
 * nothing, which is what the stochastic host already did.
 *
 * @param w the current coefficient.
 * @param threshold shrinkage magnitude; a non-positive value leaves [w] alone.
 */
internal fun softThreshold(w: Double, threshold: Double): Double = when {
    threshold <= 0.0 -> w
    w > threshold -> w - threshold
    w < -threshold -> w + threshold
    else -> 0.0
}

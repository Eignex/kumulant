package com.eignex.kumulant.stat.regression.glm

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.max

/**
 * Canonical GLM link function. Encodes everything that varies between Gaussian linear
 * regression and its GLM siblings:
 *
 *  - [invMean] turns the linear predictor `eta = bias + x . w` into the response mean
 *    (identity / sigmoid / exp).
 *  - [curvature] is the second derivative of the per-observation negative log-likelihood
 *    w.r.t. eta, used by [DiagonalRegressionStat] / [BayesianRegressionStat] to tighten
 *    precision per observation. Equals the variance function under the canonical link.
 *  - [loss] is the per-observation negative log-likelihood computed in a numerically
 *    stable way (log-trick for Logit, no overflow on either tail). Constants that
 *    don't depend on `eta` are dropped, so the absolute value is shifted but
 *    differences and sums are correct.
 *
 * Only canonical links are exposed: pairing a non-canonical link with the gradient
 * shortcut `grad_i = (mu - y) * x_i` is wrong, so restricting the type rules that out.
 */
@Serializable
sealed interface Link {
    /** Inverse link: maps the linear predictor `eta` to the response mean. */
    fun invMean(eta: Double): Double

    /** Second derivative of the per-observation negative log-likelihood at `eta`. */
    fun curvature(eta: Double): Double

    /** Per-observation negative log-likelihood (modulo `eta`-independent constants). */
    fun loss(eta: Double, y: Double): Double

    /** `mu = eta`. Gaussian likelihood with `sigma^2 = 1`. */
    @Serializable
    @SerialName("Identity")
    data object Identity : Link {
        override fun invMean(eta: Double) = eta
        override fun curvature(eta: Double) = 1.0

        // Squared error, which is twice the Gaussian negative log-likelihood: `sse` is accumulated from
        // this and `mse`/`rSquared` are defined against squared error, so halving it to match `curvature`
        // - the second derivative of the halved form - would change what those report. The two therefore
        // differ by a factor of two on this link, which matters only to a consumer comparing `sse` across
        // links or pairing `loss` with `curvature` in a line search.
        override fun loss(eta: Double, y: Double): Double {
            val r = eta - y
            return r * r
        }
    }

    /** `mu = sigmoid(eta)`. Bernoulli likelihood; expects `y in {0, 1}`. */
    @Serializable
    @SerialName("Logit")
    data object Logit : Link {
        override fun invMean(eta: Double) = sigmoid(eta)
        override fun curvature(eta: Double): Double {
            val m = sigmoid(eta)
            return m * (1.0 - m)
        }

        /** `softplus(eta) - y * eta = log(1 + exp(eta)) - y * eta`, computed stably. */
        override fun loss(eta: Double, y: Double): Double = softplus(eta) - y * eta
    }

    /** `mu = exp(eta)`. Poisson likelihood; expects `y >= 0`. */
    @Serializable
    @SerialName("Log")
    data object Log : Link {
        override fun invMean(eta: Double) = exp(eta)
        override fun curvature(eta: Double) = exp(eta)

        /** `mu - y * eta = exp(eta) - y * eta`; drops the `log(y!)` constant. */
        override fun loss(eta: Double, y: Double): Double = exp(eta) - y * eta
    }
}

/** Stable sigmoid using the positive-tail branch to avoid `exp` overflow at negative `eta`. */
internal fun sigmoid(eta: Double): Double = if (eta >= 0.0) {
    1.0 / (1.0 + exp(-eta))
} else {
    val e = exp(eta)
    e / (1.0 + e)
}

/** Stable `log(1 + exp(eta))` via `max(eta, 0) + log1p(exp(-|eta|))`. */
internal fun softplus(eta: Double): Double = max(eta, 0.0) + ln(1.0 + exp(-abs(eta)))

package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.schema.serialization.LinkSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.max

/**
 * Canonical GLM link functions used by the online regression models.
 *
 * [invMean] maps a linear predictor to the response mean. [curvature] gives the
 * negative log-likelihood curvature used to update precision. [loss] supplies the
 * fit error accumulated by the model: squared error for [Identity], negative
 * log-likelihood without response-only constants for [Logit] and [Log].
 *
 * Only canonical links are exposed so the gradient can use `(mu - y) * x`.
 */
@Serializable(with = LinkSerializer::class)
sealed interface Link {
    /** Inverse link: maps the linear predictor `eta` to the response mean. */
    fun invMean(eta: Double): Double

    /** Second derivative of the per-observation negative log-likelihood at `eta`. */
    fun curvature(eta: Double): Double

    /**
     * Per-observation fit error: squared error for [Identity], negative log-likelihood
     * up to response-only constants for [Logit] and [Log].
     */
    fun loss(eta: Double, y: Double): Double

    /**
     * Gaussian response with unit variance: mean `eta`, curvature `1`, loss `(eta - y)^2`.
     * The loss is twice the negative log-likelihood so regression results retain squared-error units.
     */
    @Serializable
    @SerialName("Identity")
    data object Identity : Link {
        override fun invMean(eta: Double) = eta
        override fun curvature(eta: Double) = 1.0

        override fun loss(eta: Double, y: Double): Double {
            val r = eta - y
            return r * r
        }
    }

    /**
     * Bernoulli response: mean `sigmoid(eta)`, curvature `mu * (1 - mu)`,
     * loss `softplus(eta) - y * eta`. Expects binary targets.
     */
    @Serializable
    @SerialName("Logit")
    data object Logit : Link {
        override fun invMean(eta: Double) = sigmoid(eta)
        override fun curvature(eta: Double): Double {
            val m = sigmoid(eta)
            return m * (1.0 - m)
        }

        override fun loss(eta: Double, y: Double): Double = softplus(eta) - y * eta
    }

    /**
     * Poisson response: mean and curvature `exp(eta)`, loss `exp(eta) - y * eta`.
     * Expects non-negative targets; omits the response-only `log(y!)` term.
     */
    @Serializable
    @SerialName("Log")
    data object Log : Link {
        override fun invMean(eta: Double) = exp(eta)
        override fun curvature(eta: Double) = exp(eta)

        override fun loss(eta: Double, y: Double): Double = exp(eta) - y * eta
    }
}

// Branch on the sign of eta so neither tail overflows exp.
internal fun sigmoid(eta: Double): Double = if (eta >= 0.0) {
    1.0 / (1.0 + exp(-eta))
} else {
    val e = exp(eta)
    e / (1.0 + e)
}

// Max-subtraction keeps exp bounded when evaluating log(1 + exp(eta)).
internal fun softplus(eta: Double): Double = max(eta, 0.0) + ln(1.0 + exp(-abs(eta)))

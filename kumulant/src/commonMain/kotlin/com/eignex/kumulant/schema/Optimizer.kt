package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.regression.AdagradOptimizer
import com.eignex.kumulant.stat.regression.AdamOptimizer
import com.eignex.kumulant.stat.regression.Optimizer
import com.eignex.kumulant.stat.regression.RmspropOptimizer
import com.eignex.kumulant.stat.regression.SgdOptimizer
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable optimizer strategy. Sealed root of [Sgd] / [Adagrad] /
 * [Rmsprop] / [Adam]; consumed by the online linear-model stats
 * ([com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat],
 * [com.eignex.kumulant.stat.regression.SoftmaxRegressionStat]) to pick the
 * per-coordinate update rule.
 *
 * A single [OptimizerSpec] materialises into one live
 * [com.eignex.kumulant.stat.regression.Optimizer] per stat. For multi-output
 * stats like [com.eignex.kumulant.stat.regression.SoftmaxRegressionStat],
 * the stat creates one optimizer per output class; each gets its own
 * per-coordinate aux state but they all share the same spec configuration.
 *
 * Pick by need:
 *
 * - [Sgd] when the per-coordinate update rate is stable and you don't need
 *   adaptive learning rates. The cheapest path; stateless.
 * - [Adagrad] when feature occurrence is sparse / power-law and rare
 *   features should learn faster than common ones. Per-coord adaptive rate.
 * - [Rmsprop] when [Adagrad]'s monotone-decreasing learning rate decays too
 *   aggressively. Exponential moving average of squared gradients.
 * - [Adam] for the general-purpose default in modern online learning.
 *   Bias-corrected first / second moments; the closest thing to "just works
 *   on most problems."
 *
 * Penalties ([com.eignex.kumulant.stat.regression.glm.Penalty]) attach to
 * [com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat] only
 * when paired with [Sgd]; the lazy-update tricks (Bottou multiplicative
 * scaling for L2, cumulative truncated gradient for L1) are SGD-specific
 * and don't extend cleanly to adaptive optimizers.
 */
@Serializable
sealed interface OptimizerSpec {
    /**
     * Build a live optimizer instance over `featureSize` coordinates at the
     * requested [Concurrency]. Each call returns a fresh optimizer with
     * empty aux state; stats call this for each weight vector they want to
     * track (one per output class for [com.eignex.kumulant.stat.regression.SoftmaxRegressionStat]).
     */
    fun materialize(featureSize: Int, concurrency: Concurrency = Concurrency.None): Optimizer
}

/**
 * Plain stochastic gradient descent. The default and the cheapest entry;
 * stateless apart from the global step counter feeding the learning-rate
 * schedule. Per-coordinate update: `w[i] -= lr(step) * weight * grad[i]`.
 *
 * Reach for [Sgd] when:
 * - You're using [com.eignex.kumulant.stat.regression.glm.Penalty.L1] or
 *   [com.eignex.kumulant.stat.regression.glm.Penalty.L2] (other optimizers
 *   don't support penalties).
 * - The per-coordinate gradient scale is roughly uniform across features
 *   (no power-law sparsity).
 * - Convergence speed matters less than memory; [Sgd]'s aux state is one
 *   global step counter.
 */
@Serializable
@SerialName("Sgd")
data class Sgd(
    /**
     * Per-step learning-rate schedule. The expression is evaluated with the
     * step counter as its `x` input; standard schedules
     * ([com.eignex.kumulant.stat.regression.glm.ConstantRate],
     * [com.eignex.kumulant.stat.regression.glm.StepDecay],
     * [com.eignex.kumulant.stat.regression.glm.ExponentialDecay]) live in
     * the GLM package. Any [ScalarExpr] works; `1.0 / (1.0 + Const(0.01) * X)`
     * for an inverse-time decay, for instance.
     */
    val learningRate: ScalarExpr = ConstantRate(1e-3),
) : OptimizerSpec {
    override fun materialize(featureSize: Int, concurrency: Concurrency): Optimizer =
        SgdOptimizer(featureSize, learningRate, concurrency)
}

/**
 * Adagrad. Per-coordinate adaptive learning rate via accumulated squared
 * gradients: `w[i] -= lr * grad[i] / sqrt(sumG2[i] + epsilon)`.
 *
 * Reach for [Adagrad] when feature occurrence is sparse and uneven;
 * power-law-distributed categorical features, rarely-seen tokens, anything
 * where you want rare features to take big steps and common features to
 * settle into small ones. The accumulating denominator makes Adagrad's
 * effective learning rate monotonically non-increasing, which is the
 * limitation [Rmsprop] addresses.
 */
@Serializable
@SerialName("Adagrad")
data class Adagrad(
    /** Base learning rate, multiplied by the per-coord `1 / sqrt(sumG2 + eps)` factor. */
    val learningRate: ScalarExpr = ConstantRate(0.01),
    /** Numerical-stability epsilon added under the square root to keep the divisor non-zero. */
    val epsilon: Double = 1e-10,
) : OptimizerSpec {
    override fun materialize(featureSize: Int, concurrency: Concurrency): Optimizer =
        AdagradOptimizer(featureSize, learningRate, epsilon, concurrency)
}

/**
 * RMSProp. Per-coordinate adaptive learning rate via an exponential moving
 * average of squared gradients: the same shape as [Adagrad] but with a
 * sliding window instead of a monotone accumulator.
 *
 * Reach for [Rmsprop] when [Adagrad]'s effective learning rate decays
 * faster than you want; non-stationary streams, online problems where the
 * data distribution drifts over the lifetime of the optimizer. [rho] near 1
 * gives a long memory (close to [Adagrad]); [rho] near 0 gives a short
 * memory.
 */
@Serializable
@SerialName("Rmsprop")
data class Rmsprop(
    /** Base learning rate, multiplied by the per-coord `1 / sqrt(emaG2 + eps)` factor. */
    val learningRate: ScalarExpr = ConstantRate(0.01),
    /** EMA decay for the squared gradient; the memory horizon is roughly `1 / (1 - rho)`. */
    val rho: Double = 0.9,
    /** Numerical-stability epsilon added under the square root. */
    val epsilon: Double = 1e-8,
) : OptimizerSpec {
    override fun materialize(featureSize: Int, concurrency: Concurrency): Optimizer =
        RmspropOptimizer(featureSize, learningRate, rho, epsilon, concurrency)
}

/**
 * Adam. Bias-corrected first and second moments per coordinate
 * (Kingma & Ba 2015); the general-purpose default in modern online
 * learning. Per-coordinate update:
 *
 * ```
 * m[i] = beta1 * m[i] + (1 - beta1) * grad[i]
 * v[i] = beta2 * v[i] + (1 - beta2) * grad[i]^2
 * mHat = m[i] / (1 - beta1^t)
 * vHat = v[i] / (1 - beta2^t)
 * w[i] -= lr * mHat / (sqrt(vHat) + epsilon)
 * ```
 *
 * Reach for [Adam] as the default. Defaults of `beta1 = 0.9`, `beta2 = 0.999`,
 * `epsilon = 1e-8` are the standard published values; the only knob most
 * callers touch is [learningRate].
 *
 * Memory cost is two state arrays of `featureSize` doubles per optimizer;
 * heavier than [Sgd] / [Adagrad] / [Rmsprop] but typically negligible
 * relative to the parameter vector itself.
 */
@Serializable
@SerialName("Adam")
data class Adam(
    /** Base learning rate applied after bias-corrected moment normalisation. */
    val learningRate: ScalarExpr = ConstantRate(0.001),
    /** First-moment EMA decay; standard published value. */
    val beta1: Double = 0.9,
    /** Second-moment EMA decay; standard published value. */
    val beta2: Double = 0.999,
    /** Numerical-stability epsilon added under the square root. */
    val epsilon: Double = 1e-8,
) : OptimizerSpec {
    override fun materialize(featureSize: Int, concurrency: Concurrency): Optimizer =
        AdamOptimizer(featureSize, learningRate, beta1, beta2, epsilon, concurrency)
}

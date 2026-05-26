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
 * Wire-portable optimizer spec. Materialises into a live
 * [com.eignex.kumulant.stat.regression.Optimizer] with the requested
 * [Concurrency]. Consumed by online-learning stats
 * ([com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat],
 * [com.eignex.kumulant.stat.regression.SoftmaxRegressionStat]) to pick the
 * per-coordinate update rule.
 */
@Serializable
sealed interface OptimizerSpec {
    /** Build the live optimizer over `featureSize` coordinates. */
    fun materialize(featureSize: Int, concurrency: Concurrency = Concurrency.None): Optimizer
}

/** Plain stochastic gradient descent. */
@Serializable
@SerialName("Sgd")
data class Sgd(
    /** Per-step learning-rate schedule. */
    val learningRate: ScalarExpr = ConstantRate(1e-3),
) : OptimizerSpec {
    override fun materialize(featureSize: Int, concurrency: Concurrency): Optimizer =
        SgdOptimizer(featureSize, learningRate, concurrency)
}

/** Adagrad: per-coord adaptive learning rate via accumulated squared gradients. */
@Serializable
@SerialName("Adagrad")
data class Adagrad(
    /** Base learning rate. */
    val learningRate: ScalarExpr = ConstantRate(0.01),
    /** Numerical-stability epsilon added under the square root. */
    val epsilon: Double = 1e-10,
) : OptimizerSpec {
    override fun materialize(featureSize: Int, concurrency: Concurrency): Optimizer =
        AdagradOptimizer(featureSize, learningRate, epsilon, concurrency)
}

/** RMSProp: exponential moving average of squared gradients with decay [rho]. */
@Serializable
@SerialName("Rmsprop")
data class Rmsprop(
    /** Base learning rate. */
    val learningRate: ScalarExpr = ConstantRate(0.01),
    /** EMA decay for the squared gradient. */
    val rho: Double = 0.9,
    /** Numerical-stability epsilon. */
    val epsilon: Double = 1e-8,
) : OptimizerSpec {
    override fun materialize(featureSize: Int, concurrency: Concurrency): Optimizer =
        RmspropOptimizer(featureSize, learningRate, rho, epsilon, concurrency)
}

/** Adam with bias-corrected first / second moments. */
@Serializable
@SerialName("Adam")
data class Adam(
    /** Base learning rate. */
    val learningRate: ScalarExpr = ConstantRate(0.001),
    /** First-moment EMA decay. */
    val beta1: Double = 0.9,
    /** Second-moment EMA decay. */
    val beta2: Double = 0.999,
    /** Numerical-stability epsilon. */
    val epsilon: Double = 1e-8,
) : OptimizerSpec {
    override fun materialize(featureSize: Int, concurrency: Concurrency): Optimizer =
        AdamOptimizer(featureSize, learningRate, beta1, beta2, epsilon, concurrency)
}

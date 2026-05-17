package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.schema.ScalarExpr
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.dot
import com.eignex.kumulant.math.forEachStored
import com.eignex.kumulant.stream.serializedLock

/**
 * Online linear regression by stochastic gradient descent on weighted MSE plus L2
 * regularisation. The cheapest of the multivariate regressors — point estimates only,
 * no posterior, fast updates.
 *
 * Update step (per observation, all coordinates `i`):
 *  ```
 *  ŷ          = bias + Σ w_i · x_i
 *  residual   = y - ŷ
 *  grad_i     = -residual · x_i + l2 · w_i
 *  w_i      ← w_i - η(step) · weight · grad_i
 *  bias     ← bias + η(step) · weight · residual
 *  ```
 *
 * Bias has its own learning-rate schedule because the intercept usually wants a much
 * faster decay than the coefficients (it dominates predictions for new arms).
 *
 * Sparse-aware: the gradient loop only touches the nonzero coordinates of [x] when
 * given a [SparseVector], skipping the `-residual · x_i` zero-contribution for dense
 * coords. L2 regularisation still applies to every coordinate, so the dense-side cost
 * is unchanged when `l2 > 0`.
 */
class SGDLinearRegression(
    override val featureSize: Int,
    val learningRate: ScalarExpr = ConstantRate(1e-3),
    val biasRate: ScalarExpr = learningRate,
    val l2: Double = 0.0,
    override val concurrency: Concurrency = Concurrency.None,
) : RegressionStat<SGDRegressionResult> {

    init { require(featureSize > 0) { "featureSize must be positive" } }

    private val lock = concurrency.serializedLock()
    private val weights = DoubleArray(featureSize)
    private var bias: Double = 0.0
    private var totalWeights: Double = 0.0
    private var step: Long = 0L
    private var sse: Double = 0.0

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) =
        lock.withLock {
            require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
            if (weight <= 0.0) return@withLock
            step++
            val eta = learningRate.eval(step.toDouble())
            val etaBias = biasRate.eval(step.toDouble())

            // ŷ via dispatched dot (sparse-aware).
            val yhat = bias + (x dot DenseVector.wrap(weights))
            val residual = y - yhat
            sse += residual * residual * weight

            if (l2 == 0.0) {
                // Sparse-friendly: only touch coords stored in x.
                val coeff = eta * weight * residual
                x.forEachStored { i, v -> weights[i] += coeff * v }
            } else {
                // L2 hits every coord; full-loop dense path regardless of x density.
                for (i in 0 until featureSize) {
                    val grad = -residual * x[i] + l2 * weights[i]
                    weights[i] -= eta * weight * grad
                }
            }
            bias += etaBias * weight * residual
            totalWeights += weight
        }

    override fun read(timestampNanos: Long): SGDRegressionResult = lock.withLock {
        SGDRegressionResult(
            weights = DenseVector.of(weights),
            bias = bias,
            totalWeights = totalWeights,
            step = step,
            sse = sse,
        )
    }

    /**
     * Sample-weighted blend of weights and bias. SGD has no second-moment information,
     * so this is an *approximation*: weight vectors from two streams are correlated
     * through their gradient trajectories in a way that an exact combine would need to
     * know about. If you need a principled merge, use [BayesianLinearRegression].
     */
    override fun merge(values: SGDRegressionResult) {
        require(values.featureSize == featureSize) {
            "merge: featureSize mismatch ${values.featureSize} vs $featureSize"
        }
        lock.withLock {
            val w1 = totalWeights
            val w2 = values.totalWeights
            val wNew = w1 + w2
            if (wNew > 0.0) {
                val other = values.weights.toDoubleArray()
                for (i in 0 until featureSize) {
                    weights[i] = (weights[i] * w1 + other[i] * w2) / wNew
                }
                bias = (bias * w1 + values.bias * w2) / wNew
            }
            totalWeights = wNew
            step += values.step
            sse += values.sse
        }
    }

    override fun reset() = lock.withLock {
        for (i in 0 until featureSize) weights[i] = 0.0
        bias = 0.0
        totalWeights = 0.0
        step = 0L
        sse = 0.0
    }

    override fun create(concurrency: Concurrency?) =
        SGDLinearRegression(featureSize, learningRate, biasRate, l2, concurrency ?: this.concurrency)
}

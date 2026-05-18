package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.forEachStored
import com.eignex.kumulant.schema.ScalarExpr
import com.eignex.kumulant.stream.getValue
import com.eignex.kumulant.stream.serializedLock
import com.eignex.kumulant.stream.SerialMode
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode

/**
 * Online linear regression by stochastic gradient descent on weighted MSE plus
 * optional [Penalty]. The cheapest of the multivariate regressors - point estimates only,
 * no posterior, fast updates.
 *
 * Update step (per observation, all coordinates `i`):
 *  ```
 *  yhat       = bias + Sum w_i * x_i
 *  residual   = y - yhat
 *  grad_i     = -residual * x_i + (penalty-specific term)
 *  w_i       -= eta(step) * weight * grad_i
 *  bias      += etaBias(step) * weight * residual
 *  ```
 *
 * Bias has its own learning-rate schedule because the intercept usually wants a much
 * faster decay than the coefficients (it dominates predictions for new arms).
 *
 * Sparse-aware at [Penalty.None]: the gradient loop only touches the nonzero coordinates
 * of [x] when given a [SparseVector], skipping the `-residual * x_i` zero-contribution
 * for dense coords. Any non-trivial [penalty] hits every coordinate, so the dense-side
 * cost is unchanged when regularisation is active.
 *
 * Penalty handling:
 *  - [Penalty.None]: plain SGD, sparse-aware.
 *  - [Penalty.L2]: gradient-form L2 (`grad_i = -residual * x_i + lambda * w_i`); full dense loop.
 *  - [Penalty.L1]: gradient SGD step followed by a proximal soft-thresholding sweep
 *    `w_i = sign(w_i) * max(0, |w_i| - eta * weight * lambda)` over every coord, which
 *    is what actually drives sparsity (subgradient L1 on its own does not).
 *
 * Concurrency is selected by access density: at [Penalty.None] the update is sparse
 * (only stored coords of `x` see writes), so [Concurrency.Relaxed] uses per-cell atomic
 * adds with no lock - HOGWILD!-style asynchronous SGD where concurrent updaters may
 * compute gradients from slightly stale weights but each write is atomic, and
 * convergence holds for the convex MSE loss. At [Penalty.L1] / [Penalty.L2] every
 * coordinate is touched every update regardless of `x`, so cells would be pure overhead
 * - [Concurrency.Relaxed] falls back to plain cells under a single lock, the same
 * machinery [Concurrency.Strict] uses. [Concurrency.None] is single-threaded in all
 * cases.
 */
class StochasticRegressionStat(
    override val featureSize: Int,
    val learningRate: ScalarExpr = ConstantRate(1e-3),
    val biasRate: ScalarExpr = learningRate,
    val penalty: Penalty = Penalty.None,
    override val concurrency: Concurrency = Concurrency.None,
) : RegressionStat<StochasticRegressionResult> {

    init { require(featureSize > 0) { "featureSize must be positive" } }

    // Sparse access (no penalty) gets HOGWILD! cells under Relaxed; dense access (L1/L2)
    // falls back to plain cells under a coarse lock since per-cell atomics buy nothing
    // when every coord is touched every update.
    private val sparseAccess = penalty == Penalty.None
    private val mode = if (sparseAccess) concurrency.welfordMode() else SerialMode
    private val lock = if (sparseAccess) concurrency.welfordLock() else concurrency.serializedLock()
    private val weightsCell = mode.newDoubleArray(featureSize)
    private val biasCell = mode.newDouble(0.0)
    private val totalWeightsCell = mode.newDouble(0.0)
    private val stepCell = mode.newLong(0L)
    private val sseCell = mode.newDouble(0.0)

    val bias: Double by biasCell
    val totalWeights: Double by totalWeightsCell
    val step: Long by stepCell
    val sse: Double by sseCell

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) =
        lock.withLock {
            require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
            if (weight <= 0.0) return@withLock
            val s = stepCell.addAndGet(1L)
            val eta = learningRate.eval(s.toDouble())
            val etaBias = biasRate.eval(s.toDouble())

            var dot = 0.0
            for (i in 0 until featureSize) dot += weightsCell.load(i) * x[i]
            val yhat = biasCell.load() + dot
            val residual = y - yhat
            sseCell.add(residual * residual * weight)

            when (val p = penalty) {
                Penalty.None -> {
                    // Sparse-friendly: only touch coords stored in x.
                    val coeff = eta * weight * residual
                    x.forEachStored { i, v -> weightsCell.add(i, coeff * v) }
                }
                is Penalty.L2 -> {
                    // L2 enters the gradient and hits every coord; full dense loop.
                    for (i in 0 until featureSize) {
                        val wi = weightsCell.load(i)
                        val grad = -residual * x[i] + p.lambda * wi
                        weightsCell.add(i, -eta * weight * grad)
                    }
                }
                is Penalty.L1 -> {
                    // Plain SGD step on every coord, then proximal soft-threshold to drive sparsity.
                    for (i in 0 until featureSize) {
                        weightsCell.add(i, eta * weight * residual * x[i])
                    }
                    val threshold = eta * weight * p.lambda
                    for (i in 0 until featureSize) softThreshold(i, threshold)
                }
            }
            biasCell.add(etaBias * weight * residual)
            totalWeightsCell.add(weight)
        }

    /** CAS-loop soft-threshold so concurrent SGD updates aren't lost under [Concurrency.Relaxed]. */
    private fun softThreshold(i: Int, threshold: Double) {
        while (true) {
            val wi = weightsCell.load(i)
            val next = when {
                wi > threshold -> wi - threshold
                wi < -threshold -> wi + threshold
                else -> 0.0
            }
            if (wi == next) return
            if (weightsCell.compareAndSet(i, wi, next)) return
        }
    }

    override fun read(timestampNanos: Long): StochasticRegressionResult = lock.withLock {
        StochasticRegressionResult(
            weights = DenseVector.of(DoubleArray(featureSize) { weightsCell.load(it) }),
            bias = biasCell.load(),
            totalWeights = totalWeightsCell.load(),
            step = stepCell.load(),
            sse = sseCell.load(),
        )
    }

    /**
     * Sample-weighted blend of weights and bias. SGD has no second-moment information,
     * so this is an *approximation*: weight vectors from two streams are correlated
     * through their gradient trajectories in a way that an exact combine would need to
     * know about. If you need a principled merge, use [BayesianRegressionStat].
     */
    override fun merge(values: StochasticRegressionResult) {
        require(values.featureSize == featureSize) {
            "merge: featureSize mismatch ${values.featureSize} vs $featureSize"
        }
        lock.withLock {
            val w1 = totalWeightsCell.load()
            val w2 = values.totalWeights
            val wNew = w1 + w2
            if (wNew > 0.0) {
                val other = values.weights.toDoubleArray()
                for (i in 0 until featureSize) {
                    val blended = (weightsCell.load(i) * w1 + other[i] * w2) / wNew
                    weightsCell.store(i, blended)
                }
                biasCell.store((biasCell.load() * w1 + values.bias * w2) / wNew)
            }
            totalWeightsCell.store(wNew)
            stepCell.add(values.step)
            sseCell.add(values.sse)
        }
    }

    override fun reset() = lock.withLock {
        for (i in 0 until featureSize) weightsCell.store(i, 0.0)
        biasCell.store(0.0)
        totalWeightsCell.store(0.0)
        stepCell.store(0L)
        sseCell.store(0.0)
    }

    override fun create(concurrency: Concurrency?) =
        StochasticRegressionStat(featureSize, learningRate, biasRate, penalty, concurrency ?: this.concurrency)
}

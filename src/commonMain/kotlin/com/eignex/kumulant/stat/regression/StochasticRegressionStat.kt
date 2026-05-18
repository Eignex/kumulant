package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.forEachStored
import com.eignex.kumulant.schema.ScalarExpr
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.getValue
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
 *  grad_i     = -residual * x_i
 *  w_i       -= eta(step) * weight * grad_i
 *  bias      += etaBias(step) * weight * residual
 *  ```
 * Regularisation is then applied via the [Penalty]-specific lazy formulation
 * described below, which keeps the per-observation cost proportional to `nnz(x)`
 * rather than to [featureSize].
 *
 * Bias has its own learning-rate schedule because the intercept usually wants a much
 * faster decay than the coefficients (it dominates predictions for new arms).
 *
 * Penalty handling (sparse-access for every variant):
 *  - [Penalty.None]: plain SGD.
 *  - [Penalty.L2]: Bottou-style multiplicative scaling - the logical weight at coord
 *    `i` is `stored[i] * scale`. Each step does `scale *= (1 - eta * weight * lambda)`
 *    (one scalar, not N) and the gradient step modifies only touched coords with
 *    `stored[i] += delta / scale`. Snapshot reads return the materialised logical
 *    weights `stored[i] * scale`. When `scale` drifts below `1e-12` it is folded
 *    back into `stored` under the lock to preserve precision.
 *  - [Penalty.L1]: truncated-gradient with cumulative threshold. A scalar
 *    `pendingThreshold` accumulates `eta * weight * lambda` per update; touching
 *    coord `i` lazily applies the threshold delta since its last touch
 *    (`lastApplied[i]`), then takes the SGD step. Untouched coords sit pending
 *    until they're either touched or materialised at read.
 *
 * Concurrency follows the Welford-coupled pattern: every cell is per-slot atomic
 * under [Concurrency.Relaxed] (HOGWILD!-style asynchronous SGD - concurrent updaters
 * may compute gradients from slightly stale weights, but each write is an atomic add
 * or CAS, and convergence holds for the convex MSE loss). Under [Concurrency.Strict]
 * a single lock fully serialises the update. [Concurrency.None] is single-threaded.
 */
class StochasticRegressionStat(
    override val featureSize: Int,
    val learningRate: ScalarExpr = ConstantRate(1e-3),
    val biasRate: ScalarExpr = learningRate,
    val penalty: Penalty = Penalty.None,
    override val concurrency: Concurrency = Concurrency.None,
) : RegressionStat<StochasticRegressionResult> {

    init { require(featureSize > 0) { "featureSize must be positive" } }

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val weightsCell = mode.newDoubleArray(featureSize)
    private val biasCell = mode.newDouble(0.0)
    private val totalWeightsCell = mode.newDouble(0.0)
    private val stepCell = mode.newLong(0L)
    private val sseCell = mode.newDouble(0.0)

    // L2 lazy-scale: actual w_i = stored[i] * scale. Only allocated when needed.
    private val l2ScaleCell: StreamDouble? = if (penalty is Penalty.L2) mode.newDouble(1.0) else null

    // L1 truncated-gradient: actual w_i = softThreshold(stored[i], pendingThreshold - lastApplied[i]).
    private val l1PendingCell: StreamDouble? = if (penalty is Penalty.L1) mode.newDouble(0.0) else null
    private val l1LastApplied: StreamDoubleArray? = if (penalty is Penalty.L1) mode.newDoubleArray(featureSize) else null

    val bias: Double by biasCell
    val totalWeights: Double by totalWeightsCell
    val step: Long by stepCell
    val sse: Double by sseCell

    /** Logical weight at coord [i]: applies the lazy [Penalty] transformation to the stored cell. */
    private fun effectiveWeight(i: Int): Double = when (val p = penalty) {
        Penalty.None -> weightsCell.load(i)
        is Penalty.L2 -> weightsCell.load(i) * l2ScaleCell!!.load()
        is Penalty.L1 -> softThreshold(weightsCell.load(i), l1PendingCell!!.load() - l1LastApplied!!.load(i))
    }

    private fun softThreshold(w: Double, threshold: Double): Double = when {
        threshold <= 0.0 -> w
        w > threshold -> w - threshold
        w < -threshold -> w + threshold
        else -> 0.0
    }

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) =
        lock.withLock {
            require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
            if (weight <= 0.0) return@withLock
            val s = stepCell.addAndGet(1L)
            val eta = learningRate.eval(s.toDouble())
            val etaBias = biasRate.eval(s.toDouble())

            // yhat uses the *effective* weight projection, but we only need it where x_i != 0.
            var dot = 0.0
            x.forEachStored { i, v -> dot += effectiveWeight(i) * v }
            val yhat = biasCell.load() + dot
            val residual = y - yhat
            sseCell.add(residual * residual * weight)

            when (val p = penalty) {
                Penalty.None -> {
                    val coeff = eta * weight * residual
                    x.forEachStored { i, v -> weightsCell.add(i, coeff * v) }
                }
                is Penalty.L2 -> {
                    // Decay the shared scale once (O(1)), then SGD only on touched coords.
                    val factor = 1.0 - eta * weight * p.lambda
                    require(factor > 0.0) {
                        "L2 decay factor must stay positive: 1 - eta*weight*lambda = $factor"
                    }
                    val scale = casMultiply(l2ScaleCell!!, factor)
                    val coeff = eta * weight * residual
                    x.forEachStored { i, v -> weightsCell.add(i, coeff * v / scale) }
                    // Fold scale back into storage when it drifts too small to keep precision.
                    if (scale < 1e-12) foldL2Scale()
                }
                is Penalty.L1 -> {
                    // Accumulate threshold once (O(1)), then per-touched-coord lazy-apply + SGD step.
                    val pending = l1PendingCell!!.addAndGet(eta * weight * p.lambda)
                    val coeff = eta * weight * residual
                    x.forEachStored { i, v ->
                        applyL1AndStep(i, pending, coeff * v)
                    }
                }
            }
            biasCell.add(etaBias * weight * residual)
            totalWeightsCell.add(weight)
        }

    /** CAS-multiply a shared scalar. Lock-free under [Concurrency.Relaxed]. */
    private fun casMultiply(cell: StreamDouble, factor: Double): Double {
        while (true) {
            val current = cell.load()
            val next = current * factor
            if (cell.compareAndSet(current, next)) return next
        }
    }

    /** Rescale stored cells by the current L2 scale and reset scale to 1.0. */
    private fun foldL2Scale() {
        val scale = l2ScaleCell!!.load()
        if (scale == 1.0) return
        if (!l2ScaleCell.compareAndSet(scale, 1.0)) return // another thread folded first
        for (i in 0 until featureSize) {
            while (true) {
                val w = weightsCell.load(i)
                if (weightsCell.compareAndSet(i, w, w * scale)) break
            }
        }
    }

    /** Apply pending L1 threshold to coord [i] and add the SGD [delta], in one CAS-loop. */
    private fun applyL1AndStep(i: Int, pending: Double, delta: Double) {
        val lastApplied = l1LastApplied!!.load(i)
        val threshold = pending - lastApplied
        while (true) {
            val stored = weightsCell.load(i)
            val thresholded = softThreshold(stored, threshold)
            val next = thresholded + delta
            if (weightsCell.compareAndSet(i, stored, next)) {
                l1LastApplied.store(i, pending)
                return
            }
        }
    }

    override fun read(timestampNanos: Long): StochasticRegressionResult = lock.withLock {
        val materialised = DoubleArray(featureSize) { effectiveWeight(it) }
        // Fold lazy state back into storage so the snapshot's invariants match the underlying cells.
        when (penalty) {
            Penalty.None -> {}
            is Penalty.L2 -> {
                for (i in 0 until featureSize) weightsCell.store(i, materialised[i])
                l2ScaleCell!!.store(1.0)
            }
            is Penalty.L1 -> {
                val pending = l1PendingCell!!.load()
                for (i in 0 until featureSize) {
                    weightsCell.store(i, materialised[i])
                    l1LastApplied!!.store(i, pending)
                }
            }
        }
        StochasticRegressionResult(
            weights = DenseVector.of(materialised),
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
                    val blended = (effectiveWeight(i) * w1 + other[i] * w2) / wNew
                    weightsCell.store(i, blended)
                    l1LastApplied?.store(i, l1PendingCell!!.load())
                }
                l2ScaleCell?.store(1.0)
                biasCell.store((biasCell.load() * w1 + values.bias * w2) / wNew)
            }
            totalWeightsCell.store(wNew)
            stepCell.add(values.step)
            sseCell.add(values.sse)
        }
    }

    override fun reset() = lock.withLock {
        for (i in 0 until featureSize) {
            weightsCell.store(i, 0.0)
            l1LastApplied?.store(i, 0.0)
        }
        l2ScaleCell?.store(1.0)
        l1PendingCell?.store(0.0)
        biasCell.store(0.0)
        totalWeightsCell.store(0.0)
        stepCell.store(0L)
        sseCell.store(0.0)
    }

    override fun create(concurrency: Concurrency?) =
        StochasticRegressionStat(featureSize, learningRate, biasRate, penalty, concurrency ?: this.concurrency)
}

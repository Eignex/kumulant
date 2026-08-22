package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorView
import com.eignex.koblas.forEachStored
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requireMergeFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.optimizer.OptimizerSpec
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stat.regression.Optimizer
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.getValue
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlin.math.pow

/**
 * Online generalised linear regression by stochastic gradient descent on the canonical
 * [Link]'s negative log-likelihood plus optional [Penalty]. The cheapest of the
 * multivariate regressors; point estimates only, no posterior, fast updates.
 *
 * The per-coordinate update rule is owned by [optimizer] ([Sgd] / [com.eignex.kumulant.schema.optimizer.Adagrad] /
 * [com.eignex.kumulant.schema.optimizer.Rmsprop] / [com.eignex.kumulant.schema.optimizer.Adam]). The bias has its own
 * [biasOptimizer] schedule because the intercept usually wants a different cadence than
 * the coefficients.
 *
 * [Penalty.L1] and [Penalty.L2] require [optimizer] (and [biasOptimizer]) to be [Sgd]; the
 * lazy-update tricks they rely on (Bottou-style multiplicative scaling for L2; cumulative
 * truncated gradient for L1) are SGD-specific. With a non-Sgd optimizer the penalty must
 * be [Penalty.None]; folding L1/L2 into Adam-class updates is left for a future refactor.
 *
 * **Use cases:** high-throughput online regression where point estimates suffice and
 * the per-update cost must stay small. Reach for [DiagonalRegressionStat] when uncertainty
 * is needed; for [BayesianRegressionStat] when the full posterior is needed.
 *
 * **Memory:** O([featureSize]); weights vector, bias, plus optimizer aux state.
 *
 * **Update:** O(nnz(x)) per observation under [Penalty.None]; the L1/L2 paths add
 * lazy-update bookkeeping with the same asymptotic cost.
 *
 * **Concurrency:** Welford-coupled per-slot atomic under [Concurrency.Relaxed]
 * (HOGWILD-style asynchronous SGD), serialised under [Concurrency.Strict] /
 * [Concurrency.HighWrite].
 *
 * @sample com.eignex.kumulant.samples.regressionUpdate
 */
class StochasticRegressionStat(
    override val featureSize: Int,
    /** Per-coordinate update rule for the weight vector. */
    val optimizer: OptimizerSpec = Sgd(),
    /** Update rule for the bias scalar. Defaults to [optimizer]. */
    val biasOptimizer: OptimizerSpec = optimizer,
    /** Regularisation applied during the gradient step; requires [Sgd] optimizers. */
    val penalty: Penalty = Penalty.None,
    /** Canonical GLM link function; [Link.Identity] gives ordinary least-squares SGD. */
    val link: Link = Link.Identity,
    override val concurrency: Concurrency = Concurrency.None,
) : RegressionStat<StochasticRegressionResult> {

    init {
        requirePositiveFeatureSize(featureSize)
        require(penalty == Penalty.None || (optimizer is Sgd && biasOptimizer is Sgd)) {
            "Penalty.L1 / Penalty.L2 require Sgd optimizers; got optimizer=${optimizer::class.simpleName}"
        }
    }

    // SGD learning-rate schedules for the penalty fast paths.
    private val sgdLearningRate: ScalarExpr? = (optimizer as? Sgd)?.learningRate
    private val sgdBiasRate: ScalarExpr? = (biasOptimizer as? Sgd)?.learningRate

    private val weightOpt: Optimizer = optimizer.materialize(featureSize, concurrency)
    private val biasOpt: Optimizer = biasOptimizer.materialize(1, concurrency)

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val weightsCell = mode.newDoubleArray(featureSize)
    private val biasCell = mode.newDouble(0.0)
    private val totalWeightsCell = mode.newDouble(0.0)
    private val stepCell = mode.newLong(0L)
    private val sseCell = mode.newDouble(0.0)

    // Logical w_i = stored[i] * scale.
    private val l2ScaleCell: StreamDouble? = if (penalty is Penalty.L2) mode.newDouble(1.0) else null

    // Logical w_i = softThreshold(stored[i], pendingThreshold - lastApplied[i]).
    private val l1PendingCell: StreamDouble? = if (penalty is Penalty.L1) mode.newDouble(0.0) else null
    private val l1LastApplied: StreamDoubleArray? =
        if (penalty is Penalty.L1) mode.newDoubleArray(featureSize) else null

    /** Live view of the running intercept. */
    val bias: Double by biasCell

    /** Live view of the cumulative observation weight folded in. */
    val totalWeights: Double by totalWeightsCell

    /** Live view of the per-observation step counter. */
    val step: Long by stepCell

    /** Live view of the accumulated per-link loss. */
    val sse: Double by sseCell

    private fun effectiveWeight(i: Int): Double = when (penalty) {
        Penalty.None -> weightsCell.load(i)

        is Penalty.L2 -> weightsCell.load(i) * requireL2Scale().load()

        is Penalty.L1 -> {
            val pending = requireL1Pending().load() - requireL1LastApplied().load(i)
            softThreshold(weightsCell.load(i), pending)
        }
    }

    private fun requireL2Scale(): StreamDouble = l2ScaleCell ?: error("l2ScaleCell only under Penalty.L2")
    private fun requireL1Pending(): StreamDouble = l1PendingCell ?: error("l1PendingCell only under Penalty.L1")
    private fun requireL1LastApplied(): StreamDoubleArray =
        l1LastApplied ?: error("l1LastApplied only under Penalty.L1")
    private fun requireSgdLearningRate(): ScalarExpr = sgdLearningRate ?: error("Sgd learning rate required")
    private fun requireSgdBiasRate(): ScalarExpr = sgdBiasRate ?: error("Sgd bias rate required")

    override fun update(x: F64VectorView, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            stepCell.addAndGet(1L)

            var dot = 0.0
            x.forEachStored { i, v -> dot += effectiveWeight(i) * v }
            val etaPred = biasCell.load() + dot
            val mu = link.invMean(etaPred)
            val negResidual = mu - y
            sseCell.add(link.loss(etaPred, y) * weight)

            when (val p = penalty) {
                Penalty.None -> {
                    weightOpt.advance()
                    biasOpt.advance()
                    x.forEachStored { i, v ->
                        val grad = negResidual * v
                        weightsCell.add(i, weightOpt.computeDelta(i, grad, weight))
                    }
                    biasCell.add(biasOpt.computeDelta(0, negResidual, weight))
                }

                is Penalty.L2 -> {
                    val eta = requireSgdLearningRate().eval(stepCell.load().toDouble())
                    val etaBias = requireSgdBiasRate().eval(stepCell.load().toDouble())
                    // Compounded over the weight rather than linearised: a weight-w observation is
                    // w unit updates, and 1 - eta*w*lambda goes non-positive on legal caller data.
                    val decay = 1.0 - eta * p.lambda
                    require(decay > 0.0) {
                        "L2 decay factor must stay positive: 1 - eta*lambda = $decay"
                    }
                    val factor = decay.pow(weight)
                    val scale = casMultiply(requireL2Scale(), factor)
                    val coeff = -eta * weight * negResidual
                    x.forEachStored { i, v -> weightsCell.add(i, coeff * v / scale) }
                    if (scale < 1e-12) foldL2Scale()
                    biasCell.add(-etaBias * weight * negResidual)
                }

                is Penalty.L1 -> {
                    val eta = requireSgdLearningRate().eval(stepCell.load().toDouble())
                    val etaBias = requireSgdBiasRate().eval(stepCell.load().toDouble())
                    val pending = requireL1Pending().addAndGet(eta * weight * p.lambda)
                    val coeff = -eta * weight * negResidual
                    x.forEachStored { i, v -> applyL1AndStep(i, pending, coeff * v) }
                    biasCell.add(-etaBias * weight * negResidual)
                }
            }
            totalWeightsCell.add(weight)
        }
    }

    private fun casMultiply(cell: StreamDouble, factor: Double): Double {
        while (true) {
            val current = cell.load()
            val next = current * factor
            if (cell.compareAndSet(current, next)) return next
        }
    }

    private fun foldL2Scale() {
        val scaleCell = requireL2Scale()
        val scale = scaleCell.load()
        if (scale == 1.0) return
        if (!scaleCell.compareAndSet(scale, 1.0)) return
        for (i in 0 until featureSize) {
            while (true) {
                val w = weightsCell.load(i)
                if (weightsCell.compareAndSet(i, w, w * scale)) break
            }
        }
    }

    private fun applyL1AndStep(i: Int, pending: Double, delta: Double) {
        val lastAppliedCell = requireL1LastApplied()
        val threshold = pending - lastAppliedCell.load(i)
        while (true) {
            val stored = weightsCell.load(i)
            val thresholded = softThreshold(stored, threshold)
            val next = thresholded + delta
            if (weightsCell.compareAndSet(i, stored, next)) {
                lastAppliedCell.store(i, pending)
                return
            }
        }
    }

    override fun read(timestampNanos: Long): StochasticRegressionResult = lock.guarded {
        val materialised = DoubleArray(featureSize) { effectiveWeight(it) }
        when (penalty) {
            Penalty.None -> {}

            is Penalty.L2 -> {
                for (i in 0 until featureSize) weightsCell.store(i, materialised[i])
                requireL2Scale().store(1.0)
            }

            is Penalty.L1 -> {
                val pending = requireL1Pending().load()
                for (i in 0 until featureSize) {
                    weightsCell.store(i, materialised[i])
                    requireL1LastApplied().store(i, pending)
                }
            }
        }
        StochasticRegressionResult(
            weights = F64DenseVector.of(materialised),
            bias = biasCell.load(),
            totalWeights = totalWeightsCell.load(),
            step = stepCell.load(),
            link = link,
            sse = sseCell.load(),
        )
    }

    /**
     * Sample-weighted blend of weights and bias. SGD has no second-moment information,
     * so this is an approximation; for principled merges use [BayesianRegressionStat].
     */
    override fun merge(values: StochasticRegressionResult) {
        requireMergeFeatureSize(values.featureSize, featureSize)
        lock.guarded {
            val w1 = totalWeightsCell.load()
            val w2 = values.totalWeights
            val wNew = w1 + w2
            if (wNew > 0.0) {
                val other = values.weights.toDoubleArray()
                for (i in 0 until featureSize) {
                    val blended = (effectiveWeight(i) * w1 + other[i] * w2) / wNew
                    weightsCell.store(i, blended)
                    l1LastApplied?.store(i, requireL1Pending().load())
                }
                l2ScaleCell?.store(1.0)
                biasCell.store((biasCell.load() * w1 + values.bias * w2) / wNew)
            }
            totalWeightsCell.store(wNew)
            stepCell.add(values.step)
            sseCell.add(values.sse)
        }
    }

    override fun reset() = lock.guarded {
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
        weightOpt.reset()
        biasOpt.reset()
    }

    override fun create(concurrency: Concurrency?) =
        StochasticRegressionStat(featureSize, optimizer, biasOptimizer, penalty, link, concurrency ?: this.concurrency)
}

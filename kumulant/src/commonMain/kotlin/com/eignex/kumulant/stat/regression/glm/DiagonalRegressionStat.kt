package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorView
import com.eignex.koblas.dot
import com.eignex.koblas.forEachStored
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock

/**
 * Generalised linear regression with a *factorised* Gaussian posterior - each
 * coefficient gets its own running precision, but cross-coefficient correlations
 * are dropped.
 *
 * Update rule (Newton-style with diagonal Hessian, canonical [Link]):
 *  ```
 *  eta               = bias + Sum w_i * x_i
 *  mu                = link.invMean(eta)
 *  curvature         = link.curvature(eta)
 *  precision_i      += weight * curvature * x_i^2
 *  w_i              -= eta_t(step) * weight * grad_i / precision_i
 *  biasPrecision    += weight * curvature
 *  bias             += eta_t(step) * weight * (y - mu) / biasPrecision
 *  ```
 * where `grad_i = (mu - y) * x_i` plus any [penalty]-specific term. Reduces to the
 * Gaussian / MSE case when `link = Link.Identity` (then `curvature = 1`, `mu = eta`).
 *
 * Posterior samples are independent per coordinate: `w_i ~ N(weights[i], 1/precision[i])`.
 *
 * Sparse-aware: precision and weight updates only fire where `x_i != 0` (matching the
 * diagonal-Hessian semantics; coordinates absent from this observation contribute no
 * curvature). Penalty handling:
 *  - [Penalty.None]: no regularisation.
 *  - [Penalty.L2]: `grad_i += lambda * w_i` on touched coords (coordinate-descent style).
 *  - [Penalty.L1]: proximal soft-thresholding on touched coords after the gradient step,
 *    threshold scaled by `eta * weight * lambda / precision_i`.
 *
 * **Use cases:** high-dimensional online regression where marginal posteriors
 * suffice (per-coordinate uncertainty without joint covariance); sparse
 * feature spaces, click-prediction style models. Reach for
 * [BayesianRegressionStat] when feature correlations matter; for
 * [StochasticRegressionStat] when SGD's even-cheaper per-update cost is
 * required.
 *
 * **Memory:** O([featureSize]); weights + per-coord precisions + bias pair.
 *
 * **Update:** O(nnz(x)) per observation; sparse-aware over the touched
 * coordinates of `x` rather than the full feature width.
 *
 * **Concurrency:** Body serialised by an internal lock under any concurrent
 * [Concurrency] level (no-op under [Concurrency.None]). Exact under every
 * level up to floating-point reorder ULPs.
 */
class DiagonalRegressionStat(
    override val featureSize: Int,
    /** Initial per-coordinate precision (inverse variance) seeded into every weight. */
    val priorPrecision: Double = 1.0,
    /** Per-step learning-rate schedule applied to coefficient updates. */
    val learningRate: ScalarExpr = ConstantRate(1.0),
    /** Regularisation applied during the gradient step; defaults to plain Newton-SGD. */
    val penalty: Penalty = Penalty.None,
    /** Canonical GLM link; [Link.Identity] is the classical Gaussian factorised posterior. */
    val link: Link = Link.Identity,
    override val concurrency: Concurrency = Concurrency.None,
) : RegressionStat<DiagonalRegressionResult> {

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(priorPrecision > 0.0) { "priorPrecision must be positive, got $priorPrecision" }
    }

    private val lock = concurrency.serializedLock()
    private val weights = DoubleArray(featureSize)
    private val precision = DoubleArray(featureSize) { priorPrecision }
    private var bias: Double = 0.0
    private var biasPrecision: Double = priorPrecision
    private var totalWeights: Double = 0.0
    private var step: Long = 0L
    private var sse: Double = 0.0

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        if (weight <= 0.0 || weight.isNaN()) return
        lock.guarded {
            step++
            val eta = learningRate.eval(step.toDouble())

            val etaPred = bias + (x dot DenseVector.wrap(weights))
            val mu = link.invMean(etaPred)
            val negResidual = mu - y
            val curvature = link.curvature(etaPred)
            sse += link.loss(etaPred, y) * weight

            // Diagonal Hessian update: only coordinates stored in x see curvature this round.
            when (val p = penalty) {
                Penalty.None -> x.forEachStored { i, v ->
                    precision[i] += weight * curvature * v * v
                    weights[i] -= eta * weight * (negResidual * v) / precision[i]
                }

                is Penalty.L2 -> x.forEachStored { i, v ->
                    precision[i] += weight * curvature * v * v
                    val grad = negResidual * v + p.lambda * weights[i]
                    weights[i] -= eta * weight * grad / precision[i]
                }

                is Penalty.L1 -> x.forEachStored { i, v ->
                    precision[i] += weight * curvature * v * v
                    weights[i] -= eta * weight * (negResidual * v) / precision[i]
                    val threshold = eta * weight * p.lambda / precision[i]
                    val wi = weights[i]
                    weights[i] = when {
                        wi > threshold -> wi - threshold
                        wi < -threshold -> wi + threshold
                        else -> 0.0
                    }
                }
            }
            biasPrecision += weight * curvature
            bias -= eta * weight * negResidual / biasPrecision
            totalWeights += weight
        }
    }

    override fun read(timestampNanos: Long): DiagonalRegressionResult = lock.guarded {
        DiagonalRegressionResult(
            weights = DenseVector.of(weights),
            bias = bias,
            biasPrecision = biasPrecision,
            totalWeights = totalWeights,
            step = step,
            precision = DenseVector.of(precision),
            link = link,
            sse = sse,
        )
    }

    /**
     * Coefficient-wise precision-weighted combine (the standard formula for
     * independent normals). Cross-feature correlations are dropped, consistent
     * with the diagonal model.
     */
    override fun merge(values: DiagonalRegressionResult) {
        require(values.featureSize == featureSize) {
            "merge: featureSize mismatch ${values.featureSize} vs $featureSize"
        }
        lock.guarded {
            val otherWeights = values.weights.toDoubleArray()
            val otherPrecision = values.precision.toDoubleArray()
            // Subtract one copy of the prior, as BayesianRegressionStat.merge does with
            // H_new = H_self + H_other - H_prior. Every replica seeds its precision at
            // priorPrecision, so pooling additively counted the prior once per replica: merging an
            // *untrained* snapshot (precision == priorPrecision, weights == 0) pulled the trained
            // weights toward zero and inflated the reported precision. The prior mean is zero, so it
            // contributes nothing to the information vector and only the denominator changes.
            for (i in 0 until featureSize) {
                val p1 = precision[i]
                val p2 = otherPrecision[i]
                val pNew = p1 + p2 - priorPrecision
                if (pNew > 0.0) {
                    weights[i] = (weights[i] * p1 + otherWeights[i] * p2) / pNew
                    precision[i] = pNew
                }
            }
            val bp = biasPrecision + values.biasPrecision - priorPrecision
            if (bp > 0.0) {
                bias = (bias * biasPrecision + values.bias * values.biasPrecision) / bp
                biasPrecision = bp
            }
            totalWeights += values.totalWeights
            step += values.step
            sse += values.sse
        }
    }

    override fun reset() = lock.guarded {
        for (i in 0 until featureSize) {
            weights[i] = 0.0
            precision[i] = priorPrecision
        }
        bias = 0.0
        biasPrecision = priorPrecision
        totalWeights = 0.0
        step = 0L
        sse = 0.0
    }

    override fun create(concurrency: Concurrency?) = DiagonalRegressionStat(
        featureSize,
        priorPrecision,
        learningRate,
        penalty,
        link,
        concurrency ?: this.concurrency,
    )
}

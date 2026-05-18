package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.dot
import com.eignex.kumulant.math.forEachStored
import com.eignex.kumulant.schema.ScalarExpr
import com.eignex.kumulant.stream.serializedLock

/**
 * Linear regression with a *factorised* Gaussian posterior - each coefficient gets
 * its own running precision, but cross-coefficient correlations are dropped.
 *
 * Update rule (Newton-style with diagonal Hessian, MSE loss):
 *  ```
 *  yhat              = bias + Sum w_i * x_i
 *  residual          = y - yhat
 *  precision_i      += weight * x_i^2
 *  w_i              -= eta(step) * weight * grad_i / precision_i
 *  biasPrecision    += weight
 *  bias             += eta(step) * weight * residual / biasPrecision
 *  ```
 * where `grad_i` is `-residual * x_i` plus the [penalty]-specific term.
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
 */
class DiagonalRegressionStat(
    override val featureSize: Int,
    val priorPrecision: Double = 1.0,
    val learningRate: ScalarExpr = ConstantRate(1.0),
    val penalty: Penalty = Penalty.None,
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

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) =
        lock.withLock {
            require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
            if (weight <= 0.0) return@withLock
            step++
            val eta = learningRate.eval(step.toDouble())

            val yhat = bias + (x dot DenseVector.wrap(weights))
            val residual = y - yhat
            sse += residual * residual * weight

            // Diagonal Hessian update: only coordinates stored in x see curvature this round.
            when (val p = penalty) {
                Penalty.None -> x.forEachStored { i, v ->
                    precision[i] += weight * v * v
                    weights[i] -= eta * weight * (-residual * v) / precision[i]
                }
                is Penalty.L2 -> x.forEachStored { i, v ->
                    precision[i] += weight * v * v
                    val grad = -residual * v + p.lambda * weights[i]
                    weights[i] -= eta * weight * grad / precision[i]
                }
                is Penalty.L1 -> x.forEachStored { i, v ->
                    precision[i] += weight * v * v
                    weights[i] -= eta * weight * (-residual * v) / precision[i]
                    val threshold = eta * weight * p.lambda / precision[i]
                    val wi = weights[i]
                    weights[i] = when {
                        wi > threshold -> wi - threshold
                        wi < -threshold -> wi + threshold
                        else -> 0.0
                    }
                }
            }
            biasPrecision += weight
            bias += eta * weight * residual / biasPrecision
            totalWeights += weight
        }

    override fun read(timestampNanos: Long): DiagonalRegressionResult = lock.withLock {
        DiagonalRegressionResult(
            weights = DenseVector.of(weights),
            bias = bias,
            biasPrecision = biasPrecision,
            totalWeights = totalWeights,
            step = step,
            precision = DenseVector.of(precision),
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
        lock.withLock {
            val otherWeights = values.weights.toDoubleArray()
            val otherPrecision = values.precision.toDoubleArray()
            for (i in 0 until featureSize) {
                val p1 = precision[i]
                val p2 = otherPrecision[i]
                val pNew = p1 + p2
                if (pNew > 0.0) {
                    weights[i] = (weights[i] * p1 + otherWeights[i] * p2) / pNew
                    precision[i] = pNew
                }
            }
            val bp = biasPrecision + values.biasPrecision
            if (bp > 0.0) {
                bias = (bias * biasPrecision + values.bias * values.biasPrecision) / bp
                biasPrecision = bp
            }
            totalWeights += values.totalWeights
            step += values.step
            sse += values.sse
        }
    }

    override fun reset() = lock.withLock {
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

    override fun create(concurrency: Concurrency?) =
        DiagonalRegressionStat(featureSize, priorPrecision, learningRate, penalty, concurrency ?: this.concurrency)
}

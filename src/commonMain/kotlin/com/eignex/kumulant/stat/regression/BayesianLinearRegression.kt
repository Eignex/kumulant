@file:Suppress("VariableNaming", "FunctionParameterNaming") // math convention: single-letter matrices L, M, etc.

package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.math.DenseMatrix
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.addOuter
import com.eignex.kumulant.math.axpy
import com.eignex.kumulant.math.cholesky
import com.eignex.kumulant.math.choleskyDowndateInPlace
import com.eignex.kumulant.math.dot
import com.eignex.kumulant.math.invertSpd
import com.eignex.kumulant.math.matVec
import com.eignex.kumulant.math.scale
import com.eignex.kumulant.math.solveSpd
import com.eignex.kumulant.schema.ScalarExpr
import com.eignex.kumulant.stream.serializedLock
import kotlin.math.sqrt

/**
 * Bayesian linear regression with a Gaussian prior on the weights and Gaussian
 * residual noise. Produces a full posterior covariance `S = H^-1` alongside the
 * point estimates. Suitable for Thompson-sampling-style bandits drawing a fresh
 * weight vector from `N(weights, exploration * S)` per round.
 *
 * Maintained incrementally via Sherman-Morrison-Woodbury on the inverse precision:
 *  ```
 *  z = S * x / sqrt(sigma^2 + xT * S * x)
 *  S = S - z * zT          (rank-1 downdate)
 *  w = w - S * (-(y - yhat) * x + lambda * w)
 *  ```
 *
 * The Cholesky factor `L` of `S` is tracked in parallel via
 * [choleskyDowndateInPlace] so `w ~ N(weights, S)` draws are an O(n^2)
 * `weights + L * u` op (no fresh Cholesky per sample). When the rank-1 downdate
 * falls outside the positive-definite cone, the factor is rebuilt from a
 * regularised covariance and the update is retried with a smaller step.
 *
 * Residual variance: `sigma^2 = 1`. Callers wanting heteroscedastic noise can
 * re-scale [y] before [update].
 */
class BayesianLinearRegression(
    override val featureSize: Int,
    val priorVariance: Double = 1.0,
    val learningRate: ScalarExpr = ConstantRate(1.0),
    val l2: Double = 0.0,
    override val concurrency: Concurrency = Concurrency.None,
) : RegressionStat<CovarianceRegressionResult> {

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(priorVariance > 0.0) { "priorVariance must be positive, got $priorVariance" }
    }

    private val lock = concurrency.serializedLock()
    private val weights = DenseVector.zero(featureSize)
    private val covariance = DenseMatrix.diagonal(featureSize, priorVariance)
    private val covarianceL = DenseMatrix.diagonal(featureSize, sqrt(priorVariance))
    private var bias: Double = 0.0
    private var biasPrecision: Double = 1.0 / priorVariance
    private var totalWeights: Double = 0.0
    private var step: Long = 0L
    private var sse: Double = 0.0

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) =
        lock.withLock {
            require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
            if (weight <= 0.0) return@withLock
            step++
            val eta = learningRate.eval(step.toDouble())

            // yhat via sparse-aware dot.
            val yhat = bias + (x dot weights)
            val residual = y - yhat
            sse += residual * residual * weight

            // z = Sum * x / sqrt(1 + xT Sum x).
            val z = matVec(covariance, x)
            val denom = sqrt(1.0 + (x dot z))
            if (denom == 0.0) return@withLock
            scale(z, 1.0 / denom)

            // Downdate the Cholesky factor; repair on instability.
            var norm = covarianceL.choleskyDowndateInPlace(z)
            if (norm > 1.0) {
                for (i in 0 until featureSize) covariance[i, i] = covariance[i, i] + 1e-5
                val Lnew = covariance.cholesky()
                for (i in 0 until featureSize)
                    for (j in 0..i) covarianceL[i, j] = Lnew[i, j]
                norm = covarianceL.choleskyDowndateInPlace(z)
                while (norm > 1.0) {
                    scale(z, 1.0 / (norm + 1e-5))
                    norm = covarianceL.choleskyDowndateInPlace(z)
                }
            }

            // Sum = Sum - z * zT  (rank-1 downdate of the covariance).
            addOuter(covariance, -1.0, z, z)

            // w = w - eta * weight * Sum * grad, where grad = -residual * x + l2 * w.
            val grad = DenseVector(featureSize)
            for (i in 0 until featureSize) grad[i] = -residual * x[i] + l2 * weights[i]
            axpy(weights, -eta * weight, matVec(covariance, grad))

            biasPrecision += weight
            bias += eta * weight * residual / biasPrecision
            totalWeights += weight
        }

    override fun read(timestampNanos: Long): CovarianceRegressionResult = lock.withLock {
        CovarianceRegressionResult(
            weights = DenseVector.of(weights.toDoubleArray()),
            bias = bias,
            biasPrecision = biasPrecision,
            totalWeights = totalWeights,
            step = step,
            covariance = DenseMatrix.of(covariance.toArray()),
            covarianceL = DenseMatrix.of(covarianceL.toArray()),
            sse = sse,
        )
    }

    /**
     * Combine two independent Gaussian posteriors over the same parameter by
     * multiplying their densities and renormalising. With zero-mean prior
     * `N(0, priorVariance * I)`, each posterior already includes one prior factor,
     * so the combined precision subtracts one copy back out:
     *
     * ```
     * H_new  = H_self + H_other - H_prior
     * b_new  = H_self * mu_self + H_other * mu_other
     * mu_new = H_new^-1 * b_new
     * S_new  = H_new^-1
     * ```
     *
     * When `H_new` drifts outside SPD the Cholesky helper's diagonal clamp catches
     * it and returns a regularised result rather than NaNs. Bias is merged the same
     * way, treating the intercept as a scalar Gaussian with zero prior mean.
     */
    override fun merge(values: CovarianceRegressionResult) {
        require(values.featureSize == featureSize) {
            "merge: featureSize mismatch ${values.featureSize} vs $featureSize"
        }
        lock.withLock {
            val n = featureSize

            // Precisions from each operand via their Cholesky factors.
            val hSelf = invertSpd(covarianceL)
            val hOther = invertSpd(values.covarianceL)

            // H_new = H_self + H_other - H_prior, where H_prior = (1/priorVariance) * I.
            val priorPrec = 1.0 / priorVariance
            val hNew = DenseMatrix(n, n)
            for (i in 0 until n) {
                for (j in 0 until n) {
                    hNew[i, j] = hSelf[i, j] + hOther[i, j] - if (i == j) priorPrec else 0.0
                }
            }

            // b = H_self * mu_self + H_other * mu_other  (mu_prior = 0 cancels its term)
            val otherWeights = values.weights.toDoubleArray()
            val selfWeights = weights.toDoubleArray()
            val b = DoubleArray(n)
            for (i in 0 until n) {
                var s = 0.0
                for (j in 0 until n) s += hSelf[i, j] * selfWeights[j] + hOther[i, j] * otherWeights[j]
                b[i] = s
            }

            // Solve H_new * mu_new = b via chol(H_new); reuse the same factor for Sum_new.
            val Lh = hNew.cholesky()
            val muNew = solveSpd(Lh, b)
            val sigmaNew = invertSpd(Lh)
            val LsigmaNew = sigmaNew.cholesky()

            for (i in 0 until n) {
                weights[i] = muNew[i]
                for (j in 0 until n) {
                    covariance[i, j] = sigmaNew[i, j]
                    covarianceL[i, j] = LsigmaNew[i, j]
                }
            }

            // Scalar bias: same precision-weighted combine, subtract one prior precision.
            val biasPriorPrec = 1.0 / priorVariance
            val bpNew = biasPrecision + values.biasPrecision - biasPriorPrec
            if (bpNew > 0.0) {
                val biasInfo = biasPrecision * bias + values.biasPrecision * values.bias
                bias = biasInfo / bpNew
                biasPrecision = bpNew
            }

            totalWeights += values.totalWeights
            step += values.step
            sse += values.sse
        }
    }

    override fun reset() = lock.withLock {
        covariance.data.fill(0.0)
        covarianceL.data.fill(0.0)
        for (i in 0 until featureSize) {
            weights[i] = 0.0
            covariance[i, i] = priorVariance
            covarianceL[i, i] = sqrt(priorVariance)
        }
        bias = 0.0
        biasPrecision = 1.0 / priorVariance
        totalWeights = 0.0
        step = 0L
        sse = 0.0
    }

    override fun create(concurrency: Concurrency?) =
        BayesianLinearRegression(featureSize, priorVariance, learningRate, l2, concurrency ?: this.concurrency)
}

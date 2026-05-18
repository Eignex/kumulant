@file:Suppress("VariableNaming", "FunctionParameterNaming") // math convention: single-letter matrices L, M, etc.

package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.math.DenseMatrix
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.MatrixView
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
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

/**
 * Bayesian linear regression with a Gaussian prior on the weights and Gaussian
 * residual noise. Produces a full posterior covariance `S = H^-1` alongside the
 * point estimates. Suitable for Thompson-sampling-style bandits drawing a fresh
 * weight vector from `N(weights, exploration * S)` per round.
 *
 * Maintained incrementally via Sherman-Morrison-Woodbury for likelihood precision
 * `weight` (variance `1/weight`):
 *  ```
 *  z       = sqrt(weight) * S * x / sqrt(1 + weight * xT * S * x)
 *  S       = S - z * zT                     (rank-1 downdate)
 *  w       = w + weight * S_new * x * (y - yhat)
 *  ```
 *
 * This is the strict closed-form conjugate Gaussian posterior update. Regularisation
 * is the Gaussian prior, controlled by [priorVariance] / `priorMean` / `priorCovariance`;
 * tighten the prior to shrink weights toward zero (or toward a target mean).
 *
 * The Cholesky factor `L` of `S` is tracked in parallel via
 * [choleskyDowndateInPlace] so `w ~ N(weights, S)` draws are an O(n^2)
 * `weights + L * u` op (no fresh Cholesky per sample). When the rank-1 downdate
 * falls outside the positive-definite cone, the factor is rebuilt from a
 * regularised covariance and the update is retried with a smaller step.
 *
 * Residual variance: `sigma^2 = 1`. Callers wanting heteroscedastic noise can
 * re-scale [y] before [update] or pass per-observation precision via `weight`.
 */
class BayesianRegressionStat(
    override val featureSize: Int,
    val priorVariance: Double = 1.0,
    override val concurrency: Concurrency = Concurrency.None,
    priorMean: VectorView? = null,
    priorCovariance: MatrixView? = null,
) : RegressionStat<CovarianceRegressionResult> {

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(priorVariance > 0.0) { "priorVariance must be positive, got $priorVariance" }
        require(priorMean == null || priorMean.size == featureSize) {
            "priorMean.size=${priorMean?.size}, expected $featureSize"
        }
        require(
            priorCovariance == null || (priorCovariance.rows == featureSize && priorCovariance.cols == featureSize)
        ) {
            "priorCovariance must be ${featureSize}x$featureSize, got ${priorCovariance?.rows}x${priorCovariance?.cols}"
        }
    }

    // Stored prior: caller-supplied or the default isotropic N(0, priorVariance * I).
    // `initialCovarianceL` is validated up front via strict (non-regularising) Cholesky
    // so a non-PD user prior throws at construction rather than silently corrupting fits.
    private val initialWeights: DoubleArray = priorMean?.toDoubleArray() ?: DoubleArray(featureSize)
    private val initialCovariance: DenseMatrix = priorCovariance
        ?.let { DenseMatrix.of(it.toArray()) }
        ?: DenseMatrix.diagonal(featureSize, priorVariance)
    private val initialCovarianceL: DenseMatrix = initialCovariance.cholesky(regularizeNonPD = false)

    // Prior precision H_prior = Sigma_prior^-1, cached so merge() can subtract one prior factor.
    private val priorPrecisionMatrix: DenseMatrix = invertSpd(initialCovarianceL)

    // priorInfo = H_prior * mu_prior, the natural-form contribution from the prior.
    private val priorInfo: DoubleArray = DoubleArray(featureSize).also { out ->
        for (i in 0 until featureSize) {
            var s = 0.0
            for (j in 0 until featureSize) s += priorPrecisionMatrix[i, j] * initialWeights[j]
            out[i] = s
        }
    }

    private val lock = concurrency.serializedLock()
    private val weights = DenseVector.of(initialWeights.copyOf())
    private val covariance = DenseMatrix.of(initialCovariance.toArray())
    private val covarianceL = DenseMatrix.of(initialCovarianceL.toArray())
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

            val yhat = bias + (x dot weights)
            val residual = y - yhat
            sse += residual * residual * weight

            // SMW rank-1 downdate for likelihood precision `weight` (variance 1/weight):
            //   S_new = S - (weight * S x xT S) / (1 + weight * xT S x)
            //         = S - z zT,  where  z = sqrt(weight) * S x / sqrt(1 + weight * xT S x).
            val z = matVec(covariance, x)
            val denom = sqrt(1.0 + weight * (x dot z))
            if (denom == 0.0) return@withLock
            scale(z, sqrt(weight) / denom)

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

            // Posterior mean update: w = w + weight * S_new * x * residual.
            val xResidual = DenseVector(featureSize)
            for (i in 0 until featureSize) xResidual[i] = residual * x[i]
            axpy(weights, weight, matVec(covariance, xResidual))

            biasPrecision += weight
            bias += weight * residual / biasPrecision
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
     * multiplying their densities and renormalising. Each posterior already includes
     * one prior factor, so the combined precision subtracts one copy back out:
     *
     * ```
     * H_new  = H_self + H_other - H_prior
     * b_new  = H_self * mu_self + H_other * mu_other - H_prior * mu_prior
     * mu_new = H_new^-1 * b_new
     * S_new  = H_new^-1
     * ```
     *
     * For a non-zero prior mean the `H_prior * mu_prior` correction is subtracted
     * from the information vector as well, otherwise the merged posterior would
     * count the prior shift twice. When `H_new` drifts outside SPD the Cholesky
     * helper's diagonal clamp catches it and returns a regularised result rather
     * than NaNs. Bias is merged the same way, treating the intercept as a scalar
     * Gaussian with zero prior mean.
     */
    override fun merge(values: CovarianceRegressionResult) {
        require(values.featureSize == featureSize) {
            "merge: featureSize mismatch ${values.featureSize} vs $featureSize"
        }
        lock.withLock {
            val n = featureSize

            val hSelf = invertSpd(covarianceL)
            val hOther = invertSpd(values.covarianceL)

            // H_new = H_self + H_other - H_prior.
            val hNew = DenseMatrix(n, n)
            for (i in 0 until n) {
                for (j in 0 until n) {
                    hNew[i, j] = hSelf[i, j] + hOther[i, j] - priorPrecisionMatrix[i, j]
                }
            }

            // b = H_self * mu_self + H_other * mu_other - H_prior * mu_prior.
            val otherWeights = values.weights.toDoubleArray()
            val selfWeights = weights.toDoubleArray()
            val b = DoubleArray(n)
            for (i in 0 until n) {
                var s = -priorInfo[i]
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
        for (i in 0 until featureSize) weights[i] = initialWeights[i]
        for (k in covariance.data.indices) {
            covariance.data[k] = initialCovariance.data[k]
            covarianceL.data[k] = initialCovarianceL.data[k]
        }
        bias = 0.0
        biasPrecision = 1.0 / priorVariance
        totalWeights = 0.0
        step = 0L
        sse = 0.0
    }

    override fun create(concurrency: Concurrency?) =
        BayesianRegressionStat(
            featureSize = featureSize,
            priorVariance = priorVariance,
            concurrency = concurrency ?: this.concurrency,
            priorMean = DenseVector.of(initialWeights.copyOf()),
            priorCovariance = DenseMatrix.of(initialCovariance.toArray()),
        )

    companion object {
        /**
         * Empirical-Bayes population prior from a set of per-instance posteriors that
         * share the same feature layout. Decomposes total variance into within-instance
         * (mean of per-instance covariances) plus between-instance (covariance of
         * per-instance means):
         *
         * ```
         * mu_pop    = weighted_mean(snapshot_i.weights)
         * Sigma_pop = mean(snapshot_i.covariance)
         *           + weighted_cov(snapshot_i.weights, mu_pop)
         * ```
         *
         * Weighting per snapshot is `snapshot.totalWeights` by default (more data =
         * tighter contribution); pass an explicit [weight] selector to override (e.g.
         * uniform weighting, or weighting by `step`). Empty input throws.
         */
        fun fitPopulationPrior(
            snapshots: List<CovarianceRegressionResult>,
            weight: (CovarianceRegressionResult) -> Double = { it.totalWeights.coerceAtLeast(1.0) },
        ): PopulationPrior {
            require(snapshots.isNotEmpty()) { "fitPopulationPrior requires at least one snapshot" }
            val n = snapshots[0].featureSize
            require(snapshots.all { it.featureSize == n }) {
                "all snapshots must share featureSize=$n"
            }

            val weights = DoubleArray(snapshots.size) { weight(snapshots[it]) }
            val wTotal = weights.sum()
            require(wTotal > 0.0) { "fitPopulationPrior: sum of weights must be positive, got $wTotal" }

            // mu_pop = weighted mean of per-instance posterior means.
            val muPop = DoubleArray(n)
            for (s in snapshots.indices) {
                val wi = weights[s]
                val w = snapshots[s].weights
                for (i in 0 until n) muPop[i] += wi * w[i]
            }
            for (i in 0 until n) muPop[i] /= wTotal

            // Sigma_pop = weighted mean of Sigma_i + weighted covariance of (mu_i - mu_pop).
            val sigmaPop = DenseMatrix(n, n)
            for (s in snapshots.indices) {
                val wi = weights[s] / wTotal
                val cov = snapshots[s].covariance
                val mu = snapshots[s].weights
                for (i in 0 until n) {
                    val di = mu[i] - muPop[i]
                    for (j in 0 until n) {
                        sigmaPop[i, j] = sigmaPop[i, j] + wi * (cov[i, j] + di * (mu[j] - muPop[j]))
                    }
                }
            }

            return PopulationPrior(
                mean = DenseVector.of(muPop),
                covariance = sigmaPop,
                instanceCount = snapshots.size,
            )
        }
    }
}

/**
 * Empirical-Bayes prior fitted across a population of related regression posteriors.
 * Hand [mean] and [covariance] straight to [BayesianRegressionStat]'s `priorMean`
 * / `priorCovariance` constructor parameters to seed a new instance.
 */
@Serializable
data class PopulationPrior(
    val mean: DenseVector,
    val covariance: DenseMatrix,
    val instanceCount: Int,
)

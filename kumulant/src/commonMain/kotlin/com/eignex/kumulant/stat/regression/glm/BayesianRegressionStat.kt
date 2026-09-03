// math convention: single-letter matrices L, M, etc.
@file:Suppress("VariableNaming", "FunctionParameterNaming", "PropertyName")
@file:OptIn(com.eignex.koblas.UnsafeKoblasApi::class)

package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.NotPositiveDefinite
import com.eignex.koblas.Workspace
import com.eignex.koblas.axpy
import com.eignex.koblas.borrow
import com.eignex.koblas.copy
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64MatrixLike
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.dense.CholeskyPolicy
import com.eignex.koblas.dense.cholesky
import com.eignex.koblas.dense.invert
import com.eignex.koblas.dense.trmv
import com.eignex.koblas.dense.trsv
import com.eignex.koblas.dot
import com.eignex.koblas.koblas
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requireMergeFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.math.choleskyUpdateInPlace
import com.eignex.kumulant.math.zeroUpperTriangle
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.Serializable

private fun F64MatrixLike.copyDenseMatrix(): F64DenseMatrix = F64DenseMatrix.wrap(
    rows,
    cols,
    DoubleArray(rows * cols) { index -> this[index % rows, index / rows] },
)

/**
 * Bayesian generalised linear regression with a Gaussian prior on the weights and a
 * canonical [Link] for the response. Produces the full joint posterior as the Cholesky
 * factor of its precision `H`, from which the covariance `S = H^-1` and every quantity
 * built on it follow by triangular solve. Suitable for Thompson-sampling-style bandits
 * drawing a fresh weight vector from `N(weights, exploration * S)` per round.
 *
 * A square-root information filter. Each observation contributes local precision
 * `w_c = weight * link.curvature(eta)` and is folded straight into the factor:
 *  ```
 *  L       <- chol(L * LT + w_c * x xT)     (rank-1 update)
 *  v       = (L * LT)^-1 * x                (two triangular solves)
 *  w       = w + weight * (y - mu) * v
 *  ```
 * Carrying `H` rather than `S` is what makes this stable: `w_c` is non-negative for
 * every canonical link, so the rank-1 update can never leave the positive-definite
 * cone, and the same event expressed on the covariance would be a Sherman-Morrison
 * downdate that can. There is no repair path because there is nothing to repair.
 *
 * Under [Link.Identity] this is the strict closed-form conjugate Gaussian posterior
 * (`curvature = 1`). Under [Link.Logit] / [Link.Log] it is the online Laplace
 * approximation - each observation tightens the posterior by the local Hessian
 * `curvature * x xT`, which is exact only at the current linear predictor. The
 * approximation tracks the true GLM posterior closely for well-identified problems.
 *
 * Regularisation is the Gaussian prior, controlled by [priorVariance] / `priorMean`
 * / `priorCovariance`; tighten the prior to shrink weights toward zero (or a target).
 * The prior covariance is factored, inverted, and factored again at construction, so a
 * non-positive-definite prior throws there rather than corrupting later fits.
 *
 * Residual variance: `sigma^2 = 1`. Callers wanting heteroscedastic noise can
 * re-scale `y` before `update()` or pass per-observation precision via `weight`.
 *
 * **Use cases:** Thompson-sampling-style linear-bandit arms (draws from
 * `N(weights, exploration · S)` per round), full-covariance online regression
 * for low-to-mid dimensions, GLM fitting where the joint posterior is needed.
 * Reach for [DiagonalRegressionStat] when only marginal posteriors are
 * required and dimensions are high.
 *
 * **Memory:** O([featureSize]^2); weights plus the precision factor and the
 * prior matrices the merge needs.
 *
 * **Update:** O([featureSize]^2) per observation; one rank-1 Cholesky update
 * and two triangular solves, with no refactorization path.
 *
 * **Concurrency:** Body serialised by an internal lock under any concurrent
 * [Concurrency] level (no-op under [Concurrency.None]). Exact under every
 * level up to floating-point reorder ULPs; throughput bound by lock
 * contention; shard and merge for higher write rates.
 */
class BayesianRegressionStat(
    override val featureSize: Int,
    /** Isotropic prior variance used when neither [priorCovariance] nor `priorMean` is supplied. */
    val priorVariance: Double = 1.0,
    /** Canonical GLM link function; [Link.Identity] is the strict closed-form Gaussian posterior. */
    val link: Link = Link.Identity,
    override val concurrency: Concurrency = Concurrency.None,
    priorMean: F64VectorLike? = null,
    priorCovariance: F64MatrixLike? = null,
) : RegressionStat<PrecisionRegressionResult> {

    init {
        requirePositiveFeatureSize(featureSize)
        require(priorVariance > 0.0) { "priorVariance must be positive, got $priorVariance" }
        require(priorMean == null || priorMean.size == featureSize) {
            "priorMean.size=${priorMean?.size}, expected $featureSize"
        }
        require(
            priorCovariance == null || (priorCovariance.rows == featureSize && priorCovariance.cols == featureSize),
        ) {
            "priorCovariance must be ${featureSize}x$featureSize, got ${priorCovariance?.rows}x${priorCovariance?.cols}"
        }
    }

    // Stored prior: caller-supplied or the default isotropic N(0, priorVariance * I).
    // `initialCovariance` survives only so `create()` can hand a child the same prior it was
    // given, rather than a round-tripped inverse of the precision.
    private val initialWeights: DoubleArray = priorMean?.toDoubleArray() ?: DoubleArray(featureSize)
    private val initialCovariance: F64DenseMatrix = priorCovariance
        ?.copyDenseMatrix()
        ?: F64DenseMatrix.diagonal(featureSize, priorVariance)

    // Prior precision H_prior = Sigma_prior^-1, cached so merge() can subtract one prior factor,
    // and its factor, which is where every update starts.
    private val priorPrecisionMatrix: F64DenseMatrix
    private val initialPrecisionL: F64DenseMatrix

    init {
        // Both factorizations are strict: a caller prior that cannot be inverted has to fail at
        // construction, not silently become a regularised neighbour of itself.
        try {
            priorPrecisionMatrix = initialCovariance.cholesky(CholeskyPolicy.Strict).invert()
            initialPrecisionL = priorPrecisionMatrix.cholesky(CholeskyPolicy.Strict).l.zeroUpperTriangle()
        } catch (error: NotPositiveDefinite) {
            throw IllegalArgumentException("priorCovariance must be positive definite", error)
        }
    }

    // priorInfo = H_prior * mu_prior, the natural-form contribution from the prior.
    private val priorInfo = DoubleArray(featureSize).also {
        priorPrecisionMatrix.multiplyInto(F64DenseVector.wrap(initialWeights), it)
    }

    private val lock = concurrency.serializedLock()
    private val weights = F64DenseVector.wrap(initialWeights.copyOf())
    private val precisionL = F64DenseMatrix.wrap(featureSize, featureSize, initialPrecisionL.data.copyOf())
    private var bias: Double = 0.0
    private var biasPrecision: Double = 1.0 / priorVariance
    private var totalWeights: Double = 0.0
    private var step: Long = 0L
    private var sse: Double = 0.0

    override fun update(x: F64VectorLike, y: Double, timestampNanos: Long, weight: Double, workspace: Workspace?) =
        updateInternal(x, y, timestampNanos, weight, workspace)

    @Suppress("UnusedParameter")
    private fun updateInternal(
        x: F64VectorLike,
        y: Double,
        _timestampNanos: Long,
        weight: Double,
        workspace: Workspace?,
    ) {
        x.requireFeatureSize(featureSize)
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            step++

            val etaPred = bias + (x dot weights)
            val mu = link.invMean(etaPred)
            val residual = y - mu
            val curvature = link.curvature(etaPred)
            sse += link.loss(etaPred, y) * weight

            // For canonical-link GLMs the per-observation precision is `weight * curvature`:
            // 1 under Identity gives the exact conjugate update; for Logit / Log this is the local
            // Laplace approximation around the current linear predictor (not the strict closed-form
            // Bayesian update). `Link.Log.curvature` is `exp(eta)`, which overflows for a large
            // linear predictor; an infinite w_c would write Infinity through the factor and take
            // every later prediction with it, so the observation is skipped instead.
            val wc = weight * curvature
            if (!wc.isFinite()) return@guarded

            // H <- H + w_c * x xT, as a rank-1 update of its factor; a zero w_c leaves it alone.
            precisionL.choleskyUpdateInPlace(x, wc, workspace)

            workspace.borrow(featureSize) { hx ->
                // Posterior mean update: w += (weight * residual) * H_new^-1 * x, solved against
                // the factor just updated rather than through a materialised covariance.
                val solved = F64DenseVector.wrap(hx)
                copy(x, solved)
                precisionL.trsv(hx, lower = true)
                precisionL.trsv(hx, lower = true, transpose = true)
                weights.axpy(weight * residual, solved)

                biasPrecision += wc
                bias += weight * residual / biasPrecision
                totalWeights += weight
            }
        }
    }

    override fun read(timestampNanos: Long): PrecisionRegressionResult = lock.guarded {
        PrecisionRegressionResult(
            weights = F64DenseVector.wrap(weights.data.copyOf()),
            bias = bias,
            biasPrecision = biasPrecision,
            totalWeights = totalWeights,
            step = step,
            precisionL = F64DenseMatrix.wrap(featureSize, featureSize, precisionL.data.copyOf()),
            link = link,
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
     * ```
     *
     * Both operands already carry their precision factor, so `H` comes back by `syrk`
     * and the information vectors by two triangular products each; the factor of
     * `H_new` is the new state, so nothing is inverted and nothing is factored twice.
     * For a non-zero prior mean the `H_prior * mu_prior` correction is subtracted
     * from the information vector as well, otherwise the merged posterior would
     * count the prior shift twice. When `H_new` drifts outside SPD,
     * `CholeskyPolicy.Regularize` clamps the offending pivot and factors a nearby
     * matrix rather than returning NaNs. Bias is merged the same way, treating
     * the intercept as a scalar Gaussian with zero prior mean.
     */
    override fun merge(values: PrecisionRegressionResult, workspace: Workspace?) = mergeInternal(values, workspace)

    private fun mergeInternal(values: PrecisionRegressionResult, workspace: Workspace?) {
        requireMergeFeatureSize(values.featureSize, featureSize)
        lock.guarded {
            val n = featureSize

            // H_new = H_self + H_other - H_prior, accumulated in the lower triangle, which is the
            // only one the Cholesky below reads. `syrk` takes each factor as a general matrix, so
            // it depends on the strict upper triangle being zero, which is what `precisionL`
            // promises and what `zeroUpperTriangle` keeps true on every write to it.
            val hNew = F64DenseMatrix.zero(n, n)
            koblas.syrk(1.0, precisionL, transpose = false, 0.0, hNew, lower = true, workspace = workspace)
            koblas.syrk(1.0, values.precisionL, transpose = false, 1.0, hNew, lower = true, workspace = workspace)
            for (j in 0 until n) {
                for (i in j until n) hNew[i, j] = hNew[i, j] - priorPrecisionMatrix[i, j]
            }

            // b = H_self * mu_self + H_other * mu_other - H_prior * mu_prior, each product taken as
            // L * (LT * mu) so the precision never has to be formed for it.
            workspace.borrow(n) { b ->
                for (i in 0 until n) b[i] = weights[i]
                precisionL.trmv(b, lower = true, transpose = true)
                precisionL.trmv(b, lower = true)
                workspace.borrow(n) { other ->
                    for (i in 0 until n) other[i] = values.weights[i]
                    values.precisionL.trmv(other, lower = true, transpose = true)
                    values.precisionL.trmv(other, lower = true)
                    for (i in 0 until n) b[i] += other[i] - priorInfo[i]
                }

                // Solve H_new * mu_new = b via chol(H_new); that factor is the merged state.
                val hChol = hNew.cholesky(CholeskyPolicy.Regularize())
                koblas.solveInto(hChol, b, b)
                val lNew = hChol.l.zeroUpperTriangle()

                for (i in 0 until n) {
                    weights[i] = b[i]
                    for (j in 0 until n) precisionL[i, j] = lNew[i, j]
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

    override fun reset() = lock.guarded {
        for (i in 0 until featureSize) weights[i] = initialWeights[i]
        for (k in precisionL.data.indices) precisionL.data[k] = initialPrecisionL.data[k]
        bias = 0.0
        biasPrecision = 1.0 / priorVariance
        totalWeights = 0.0
        step = 0L
        sse = 0.0
    }

    override fun create(concurrency: Concurrency?) = BayesianRegressionStat(
        featureSize = featureSize,
        priorVariance = priorVariance,
        link = link,
        concurrency = concurrency ?: this.concurrency,
        priorMean = F64DenseVector.wrap(initialWeights.copyOf()),
        priorCovariance = F64DenseMatrix.wrap(featureSize, featureSize, initialCovariance.data.copyOf()),
    )

    /** Empirical-Bayes / hierarchical helpers that operate on populations of fitted snapshots. */
    companion object {
        /**
         * Empirical-Bayes population prior from a set of per-instance posteriors that
         * share the same feature layout. Decomposes total variance into within-instance
         * (mean of per-instance covariances) plus between-instance (covariance of
         * per-instance means):
         *
         * ```
         * mu_pop    = weighted_mean(snapshot_i.weights)
         * Sigma_pop = mean(snapshot_i.covariance())
         *           + weighted_cov(snapshot_i.weights, mu_pop)
         * ```
         *
         * Weighting per snapshot is `snapshot.totalWeights` by default (more data =
         * tighter contribution); pass an explicit [weight] selector to override (e.g.
         * uniform weighting, or weighting by `step`). Empty input throws. Each snapshot
         * is inverted once, so this is O(instances · featureSize^3); it is a periodic
         * refit, not a per-observation path.
         */
        fun fitPopulationPrior(
            snapshots: List<PrecisionRegressionResult>,
            weight: (PrecisionRegressionResult) -> Double = { it.totalWeights.coerceAtLeast(1.0) },
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
            val sigmaPop = F64DenseMatrix.zero(n, n)
            for (s in snapshots.indices) {
                val wi = weights[s] / wTotal
                val cov = snapshots[s].covariance()
                val mu = snapshots[s].weights
                for (i in 0 until n) {
                    val di = mu[i] - muPop[i]
                    for (j in 0 until n) {
                        sigmaPop[i, j] = sigmaPop[i, j] + wi * (cov[i, j] + di * (mu[j] - muPop[j]))
                    }
                }
            }

            return PopulationPrior(
                mean = F64DenseVector.of(muPop),
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
    /** Population mean of the per-instance posterior means. */
    val mean: F64DenseVector,
    /** Population covariance: within-instance posterior + between-instance mean spread. */
    val covariance: F64DenseMatrix,
    /** Number of per-instance posteriors that contributed to this prior. */
    val instanceCount: Int,
)

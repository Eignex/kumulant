// math convention: single-letter matrices L, M, etc.
@file:Suppress("VariableNaming", "FunctionParameterNaming", "PropertyName")
@file:OptIn(com.eignex.koblas.UnsafeKoblasApi::class)

package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.ContiguousVector
import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.Matrix
import com.eignex.koblas.Vector
import com.eignex.koblas.Workspace
import com.eignex.koblas.axpy
import com.eignex.koblas.borrow
import com.eignex.koblas.copy
import com.eignex.koblas.dot
import com.eignex.koblas.gemvInto
import com.eignex.koblas.koblas
import com.eignex.koblas.syr
import com.eignex.koblas.trmv
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.hasNonFiniteStored
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requireMergeFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.math.CholeskyPolicy
import com.eignex.kumulant.math.NotPositiveDefinite
import com.eignex.kumulant.math.cholesky
import com.eignex.kumulant.math.choleskyInto
import com.eignex.kumulant.math.choleskyInverse
import com.eignex.kumulant.math.choleskyInvertInto
import com.eignex.kumulant.math.choleskyRankUpdate
import com.eignex.kumulant.math.choleskySolveInto
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.Serializable

private fun Matrix.copyDenseMatrix(): DenseMatrix = DenseMatrix.wrap(
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
 * An observation with a non-finite feature is skipped whole, since it would leave the
 * factor non-finite and every later merge unable to factor it. Non-finite targets follow
 * the arithmetic: the factor stays finite while weights, bias and `sse` become `NaN`.
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
 * prior matrices the merge needs, and the pooled scratch the first update or
 * merge borrows, which is then held for the life of the stat so later calls
 * reuse it.
 *
 * **Update:** O([featureSize]^2) per observation; one rank-1 Cholesky update
 * and two triangular solves, with no refactorization path.
 *
 * **Concurrency:** Body serialised by an internal lock under any concurrent
 * [Concurrency] level (no-op under [Concurrency.None]). Exact under every
 * level up to floating-point reorder ULPs; throughput bound by lock
 * contention; shard and merge for higher write rates. The update and merge
 * scratch is owned per stat and guarded by that lock alone, so a
 * [Concurrency.None] stat updated or merged from two threads can fail inside
 * the linear-algebra layer rather than only drift.
 */
class BayesianRegressionStat(
    override val featureSize: Int,
    /** Isotropic prior variance used when neither [priorCovariance] nor `priorMean` is supplied. */
    val priorVariance: Double = StatDefaults.BAYESIAN_REGRESSION_PRIOR_VARIANCE,
    /** Canonical GLM link function; [Link.Identity] is the strict closed-form Gaussian posterior. */
    val link: Link = StatDefaults.regressionLink,
    override val concurrency: Concurrency = Concurrency.None,
    priorMean: Vector? = null,
    priorCovariance: Matrix? = null,
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

    // Keep the supplied covariance for replicas to preserve the prior without an inverse round-trip.
    private val initialWeights: DoubleArray = priorMean?.toDoubleArray() ?: DoubleArray(featureSize)
    private val initialCovariance: DenseMatrix = priorCovariance
        ?.copyDenseMatrix()
        ?: DenseMatrix.diagonal(featureSize, priorVariance)

    // Cache prior precision and its factor for updates and prior-corrected merges.
    private val priorPrecisionMatrix: DenseMatrix
    private val initialPrecisionL: DenseMatrix

    init {
        // Caller priors must be positive definite; strict factorization preserves them exactly.
        try {
            priorPrecisionMatrix = initialCovariance.cholesky(CholeskyPolicy.Strict).choleskyInverse()
            initialPrecisionL = priorPrecisionMatrix.cholesky(CholeskyPolicy.Strict)
        } catch (error: NotPositiveDefinite) {
            throw IllegalArgumentException("priorCovariance must be positive definite", error)
        }
    }

    // priorInfo = H_prior * mu_prior, the natural-form contribution from the prior.
    private val priorInfo = DoubleArray(featureSize).also {
        priorPrecisionMatrix.gemvInto(1.0, DenseVector.wrap(initialWeights), 0.0, it)
    }

    private val lock = concurrency.serializedLock()

    // Each stat owns a workspace protected by serializedLock under concurrent use; replicas have independent
    // workspaces.
    private val workspace = Workspace()
    private val weights = DenseVector.wrap(initialWeights.copyOf())
    private val precisionL = DenseMatrix.wrap(featureSize, featureSize, initialPrecisionL.values.copyOf())

    private var bias: Double = 0.0
    private var biasPrecision: Double = 1.0 / priorVariance
    private var totalWeights: Double = 0.0
    private var step: Long = 0L
    private var sse: Double = 0.0

    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) =
        updateInternal(x, y, timestampNanos, weight)

    @Suppress("UnusedParameter")
    private fun updateInternal(x: Vector, y: Double, _timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        if (weight.isNotPositiveWeight()) return
        if (x.hasNonFiniteStored()) return
        lock.guarded {
            val etaPred = bias + (x dot weights)
            val curvature = link.curvature(etaPred)

            // Precision uses weight * curvature: exact under Identity, a local Laplace approximation under Logit/Log.
            // Non-finite curvature is skipped whole to preserve the precision factor.
            val wc = weight * curvature
            if (!wc.isFinite()) return@guarded

            step++
            val residual = y - link.invMean(etaPred)
            sse += link.loss(etaPred, y) * weight

            // The rank-1 update preserves the scattered x for the following solve; scattering also speeds sparse
            // access.
            workspace.borrow(featureSize) { hx ->
                val solved = DenseVector.wrap(hx)
                copy(x, solved)

                // H <- H + w_c * x xT, as a rank-1 update of its factor; a zero w_c leaves it alone.
                if (wc > 0.0) precisionL.choleskyRankUpdate(hx, wc, workspace)

                // Posterior mean update: w += (weight * residual) * H_new^-1 * x, solved against
                // the factor just updated rather than through a materialised covariance.
                precisionL.choleskySolveInto(hx, hx, workspace)
                weights.axpy(weight * residual, solved)
            }

            biasPrecision += wc
            bias += weight * residual / biasPrecision
            totalWeights += weight
        }
    }

    override fun read(timestampNanos: Long): PrecisionRegressionResult = lock.guarded {
        PrecisionRegressionResult(
            weights = DenseVector.wrap(weights.values.copyOf()),
            bias = bias,
            biasPrecision = biasPrecision,
            totalWeights = totalWeights,
            step = step,
            precisionL = DenseMatrix.wrap(featureSize, featureSize, precisionL.values.copyOf()),
            link = link,
            sse = sse,
        )
    }

    override fun validateMerge(values: PrecisionRegressionResult) {
        requireMergeFeatureSize(values.featureSize, featureSize)
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
    override fun merge(values: PrecisionRegressionResult) = mergeInternal(values)

    private fun mergeInternal(values: PrecisionRegressionResult) {
        validateMerge(values)
        lock.guarded {
            val n = featureSize

            // H_new = H_self + H_other - H_prior, accumulated in the lower triangle, which is the
            // only one the Cholesky below reads. `syrk` takes each factor as a general matrix, so
            // it depends on the strict upper triangle being zero. Factorization clears it, and
            // rank-1 updates preserve it.
            val hNew = DenseMatrix.zero(n, n)
            koblas.syrk(1.0, precisionL, transpose = false, 0.0, hNew, lower = true, workspace = workspace)
            koblas.syrk(1.0, values.precisionL, transpose = false, 1.0, hNew, lower = true, workspace = workspace)
            for (j in 0 until n) {
                for (i in j until n) hNew[i, j] = hNew[i, j] - priorPrecisionMatrix[i, j]
            }

            // b = H_self * mu_self + H_other * mu_other - H_prior * mu_prior, each product taken as
            // L * (LT * mu) so the precision never has to be formed for it.
            workspace.borrow(n) { b ->
                for (i in 0 until n) b[i] = weights[i]
                precisionL.trmv(b, lower = true, transpose = true, workspace = workspace)
                precisionL.trmv(b, lower = true, workspace = workspace)
                workspace.borrow(n) { other ->
                    for (i in 0 until n) other[i] = values.weights[i]
                    values.precisionL.trmv(other, lower = true, transpose = true, workspace = workspace)
                    values.precisionL.trmv(other, lower = true, workspace = workspace)
                    for (i in 0 until n) b[i] += other[i] - priorInfo[i]
                }

                // Solve H_new * mu_new = b via chol(H_new); that factor is the merged state.
                hNew.choleskyInto(hNew, CholeskyPolicy.Regularize(), workspace)
                hNew.choleskySolveInto(b, b, workspace)

                for (i in 0 until n) {
                    weights[i] = b[i]
                    for (j in 0 until n) precisionL[i, j] = hNew[i, j]
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
        for (k in precisionL.values.indices) precisionL.values[k] = initialPrecisionL.values[k]
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
        priorMean = DenseVector.wrap(initialWeights.copyOf()),
        priorCovariance = DenseMatrix.wrap(featureSize, featureSize, initialCovariance.values.copyOf()),
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
            val sigmaPop = DenseMatrix.zero(n, n)
            val cov = DenseMatrix.zero(n, n)
            val sigmaFlat = DenseVector.wrap(sigmaPop.values)
            val covFlat = DenseVector.wrap(cov.values)
            val deviation = DoubleArray(n)
            val deviationVector = DenseVector.wrap(deviation)
            // A call-local workspace reuses inversion scratch across population snapshots.
            val scratch = Workspace()
            for (s in snapshots.indices) {
                val wi = weights[s] / wTotal
                snapshots[s].precisionL.choleskyInvertInto(cov, scratch)
                val mu = snapshots[s].weights
                sigmaFlat.axpy(wi, covFlat)
                for (i in 0 until n) deviation[i] = mu[i] - muPop[i]
                sigmaPop.syr(wi, deviationVector, workspace = scratch)
            }
            // Reflect the lower triangle after syr to publish a symmetric covariance.
            for (i in 0 until n) {
                for (j in i + 1 until n) sigmaPop[i, j] = sigmaPop[j, i]
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
    /** Population mean of the per-instance posterior means. */
    val mean: ContiguousVector,
    /** Population covariance: within-instance posterior + between-instance mean spread. */
    val covariance: DenseMatrix,
    /** Number of per-instance posteriors that contributed to this prior. */
    val instanceCount: Int,
)

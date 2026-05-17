package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.nextNormal
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Stateless multivariate sampler over a [LinearRegressionResult] snapshot — reads
 * weights (and whichever uncertainty-quantification fields the concrete snapshot
 * carries) and returns a fresh draw of the weight vector, scaled by an
 * `exploration` multiplier so callers can dial the posterior variance up or down
 * per round.
 *
 * Lives next to the regression results because Thompson sampling is one of several
 * ways to consume a posterior — Bayesian optimisation will combine these samplers
 * with acquisition functions (UCB, EI, PI) that read the same snapshots. The
 * univariate-bandit `kumulant.bandit.Posterior` family is sibling but disjoint:
 * different result shape, different math.
 *
 * Sealed + `@Serializable` so a `(regression, posterior)` configuration is wire-
 * portable via skema's polymorphic discriminator.
 */
@Serializable
sealed interface LinearPosterior<R : LinearRegressionResult> {
    /** Draw a weight vector from the posterior at `exploration` variance scale.
     *  `exploration = 0.0` is the point estimate; `1.0` is the calibrated posterior. */
    fun sample(snapshot: R, rng: Random, exploration: Double = 1.0): VectorView
}

/**
 * Point estimate plus optional isotropic Gaussian noise. SGD models have no posterior
 * variance to draw from — `exploration` lets callers add a constant std-dev shake on
 * top of the point estimate for exploration's sake (matches the legacy `SGDLinearModel`
 * `sample` semantics).
 */
@Serializable
@SerialName("PointPosterior")
data object PointPosterior : LinearPosterior<SGDRegressionResult> {
    override fun sample(snapshot: SGDRegressionResult, rng: Random, exploration: Double): VectorView {
        if (exploration <= 0.0) return snapshot.weights
        val n = snapshot.weights.size
        val sd = sqrt(exploration)
        val out = DoubleArray(n)
        for (i in 0 until n) out[i] = rng.nextNormal(snapshot.weights[i], sd)
        return DenseVector.of(out)
    }
}

/**
 * Per-coordinate Gaussian: each `w_i ~ N(weights[i], exploration / precision[i])`.
 * Cheap O(n) draws; ignores cross-feature correlations.
 */
@Serializable
@SerialName("FactorisedGaussian")
data object FactorisedGaussian : LinearPosterior<DiagonalRegressionResult> {
    override fun sample(snapshot: DiagonalRegressionResult, rng: Random, exploration: Double): VectorView {
        val n = snapshot.weights.size
        val out = DoubleArray(n)
        for (i in 0 until n) {
            val sd = sqrt(exploration / snapshot.precision[i])
            out[i] = rng.nextNormal(snapshot.weights[i], sd)
        }
        return DenseVector.of(out)
    }
}

/**
 * Full multivariate-Gaussian draw `w ~ N(weights, exploration · Σ)` via the
 * pre-computed Cholesky factor `L` carried in the snapshot:
 *
 * `w = weights + sqrt(exploration) · L · u` where `u ~ N(0, I)`.
 *
 * O(n²) per draw; no fresh Cholesky decomposition required.
 */
@Serializable
@SerialName("MultivariateGaussian")
data object MultivariateGaussian : LinearPosterior<CovarianceRegressionResult> {
    override fun sample(snapshot: CovarianceRegressionResult, rng: Random, exploration: Double): VectorView {
        val n = snapshot.weights.size
        val sd = sqrt(exploration)
        val u = DoubleArray(n) { rng.nextNormal(0.0, sd) }
        val out = DoubleArray(n)
        for (i in 0 until n) {
            var s = snapshot.weights[i]
            for (j in 0..i) s += snapshot.covarianceL[i, j] * u[j]
            out[i] = s
        }
        return DenseVector.of(out)
    }
}

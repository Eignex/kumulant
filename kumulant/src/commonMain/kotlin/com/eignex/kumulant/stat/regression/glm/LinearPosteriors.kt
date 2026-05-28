package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.dot
import com.eignex.kumulant.math.matVec
import com.eignex.kumulant.math.nextNormal
import com.eignex.kumulant.stat.regression.RegressionPosterior
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Stateless multivariate sampler over a [LinearRegressionResult] snapshot. Reads
 * the weights (and whichever uncertainty fields the concrete snapshot carries) and
 * returns a fresh weight-vector draw, scaled by `exploration` so callers can dial
 * the posterior variance per round.
 *
 * Lives next to the regression results because Thompson sampling is one of several
 * ways to consume the posterior; Bayesian optimisation will combine these samplers
 * with acquisition functions (UCB, EI, PI) reading the same snapshots. The
 * univariate-bandit [com.eignex.kumulant.bandit.univariate.Posterior] family is a sibling but
 * disjoint: different result shape, different math.
 *
 * Sealed + `@Serializable` so a `(regression, posterior)` configuration is wire
 * portable via skema's polymorphic discriminator.
 */
@Serializable
sealed interface LinearPosterior<R : LinearRegressionResult> : RegressionPosterior<R> {
    /** Draw a weight vector from the posterior at `exploration` variance scale.
     *  `exploration = 0.0` collapses to the point estimate; `1.0` is the calibrated posterior. */
    fun sample(snapshot: R, rng: Random, exploration: Double = 1.0): VectorView

    /**
     * Score a query point [x] under a fresh posterior draw. Parallels
     * [com.eignex.kumulant.bandit.univariate.BanditPolicy.evaluate] for the multivariate
     * setting: an outer "pick the best x" loop calls this once per candidate.
     *
     * Default is `bias + (x dot sample(...))`. Concrete subtypes may override with
     * a specialised formula (e.g. drawing only `xT * Sigma * x` worth of variance
     * instead of the full weight vector).
     */
    override fun evaluate(snapshot: R, x: VectorView, rng: Random, exploration: Double): Double =
        snapshot.bias + (x dot sample(snapshot, rng, exploration))
}

/**
 * Point estimate plus optional isotropic Gaussian noise. SGD models have no
 * posterior variance to draw from; `exploration` adds a constant std-dev shake on
 * top of the point estimate.
 */
@Serializable
@SerialName("PointPosterior")
data object PointPosterior : LinearPosterior<StochasticRegressionResult> {
    override fun sample(snapshot: StochasticRegressionResult, rng: Random, exploration: Double): VectorView {
        if (exploration <= 0.0) return snapshot.weights
        val n = snapshot.weights.size
        val sd = sqrt(exploration)
        val out = DoubleArray(n)
        for (i in 0 until n) out[i] = rng.nextNormal(snapshot.weights[i], sd)
        return DenseVector.of(out)
    }

    /** Closes to `predict(x) + sd * ||x|| * N(0,1)` since the per-coord noise terms
     *  are iid; one Gaussian draw instead of one per coordinate. */
    override fun evaluate(
        snapshot: StochasticRegressionResult,
        x: VectorView,
        rng: Random,
        exploration: Double,
    ): Double {
        val mean = snapshot.predict(x)
        if (exploration <= 0.0) return mean
        val xNormSq = x dot x
        return mean + sqrt(exploration * xNormSq) * rng.nextNormal()
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

    /** Sum of independent normals: `predict(x) + sqrt(exploration * Sum x_i^2 / precision[i]) * N(0,1)`. */
    override fun evaluate(snapshot: DiagonalRegressionResult, x: VectorView, rng: Random, exploration: Double): Double {
        val mean = snapshot.predict(x)
        var variance = 0.0
        for (i in 0 until x.size) {
            val xi = x[i]
            variance += xi * xi / snapshot.precision[i]
        }
        return mean + sqrt(exploration * variance) * rng.nextNormal()
    }
}

/**
 * Full multivariate-Gaussian draw `w ~ N(weights, exploration * Sum)` via the
 * pre-computed Cholesky factor `L` carried in the snapshot:
 *
 * `w = weights + sqrt(exploration) * L * u` where `u ~ N(0, I)`.
 *
 * O(n^2) per draw; no fresh Cholesky decomposition required.
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

    /** Closes to `predict(x) + sqrt(exploration * xT * Sigma * x) * N(0,1)`; one
     *  `matVec` and one `dot` instead of sampling the full weight vector. */
    override fun evaluate(
        snapshot: CovarianceRegressionResult,
        x: VectorView,
        rng: Random,
        exploration: Double,
    ): Double {
        val mean = snapshot.predict(x)
        val sigmaX = matVec(snapshot.covariance, x)
        val variance = x dot sigmaX
        return mean + sqrt(exploration * variance) * rng.nextNormal()
    }
}

/**
 * LinUCB-style confidence-bound scoring: `predict(x) + exploration * sqrt(xT * Sigma * x)`.
 * Deterministic given the snapshot; no random draw at evaluate time; so the
 * `exploration` parameter here plays the role of LinUCB's `alpha` (confidence-bound
 * width), not the variance scale used by Thompson-style posteriors. [sample] returns
 * the snapshot's mean weights since UCB has no per-arm randomization; callers that
 * want sampled weights should pair with [MultivariateGaussian] instead.
 */
@Serializable
@SerialName("LinUcb")
data object LinUcb : LinearPosterior<CovarianceRegressionResult> {
    override fun sample(snapshot: CovarianceRegressionResult, rng: Random, exploration: Double): VectorView =
        snapshot.weights

    override fun evaluate(
        snapshot: CovarianceRegressionResult,
        x: VectorView,
        rng: Random,
        exploration: Double,
    ): Double {
        val mean = snapshot.predict(x)
        val sigmaX = matVec(snapshot.covariance, x)
        val variance = x dot sigmaX
        return mean + exploration * sqrt(variance)
    }
}

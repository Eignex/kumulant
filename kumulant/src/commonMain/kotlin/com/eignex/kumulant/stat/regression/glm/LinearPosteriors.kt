package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.Workspace
import com.eignex.koblas.axpy
import com.eignex.koblas.borrow
import com.eignex.koblas.copy
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.dense.trsv
import com.eignex.koblas.dot
import com.eignex.koblas.forEachStored
import com.eignex.koblas.norm2
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
    fun sample(snapshot: R, rng: Random, exploration: Double = 1.0): F64VectorLike

    /** Writes an owned posterior draw into [destination]. */
    fun sampleInto(snapshot: R, rng: Random, destination: DoubleArray, exploration: Double = 1.0) {
        require(destination.size == snapshot.weights.size) {
            "destination size ${destination.size} must match weights size ${snapshot.weights.size}"
        }
        val sample = sample(snapshot, rng, exploration)
        for (i in destination.indices) destination[i] = sample[i]
    }

    /**
     * Score a query point [x] under a fresh posterior draw. Parallels
     * [com.eignex.kumulant.bandit.univariate.BanditPolicy.evaluate] for the multivariate
     * setting: an outer "pick the best x" loop calls this once per candidate.
     *
     * Default is `invMean(bias + (x dot sample(...)))`. Concrete subtypes may override
     * with a specialised formula (e.g. drawing only `xT * Sigma * x` worth of variance
     * instead of the full weight vector).
     *
     * The draw is uncertainty over the coefficients, which live in the linear-predictor
     * space, so the noise is added before the inverse link, never after: a score has to
     * come back on the same scale as the reward being maximised.
     */
    override fun evaluate(
        snapshot: R,
        x: F64VectorLike,
        rng: Random,
        exploration: Double,
        workspace: Workspace?,
    ): Double = snapshot.link.invMean(snapshot.bias + (x dot sample(snapshot, rng, exploration)))
}

/**
 * Point estimate plus optional isotropic Gaussian noise. SGD models have no
 * posterior variance to draw from; `exploration` adds a constant std-dev shake on
 * top of the point estimate.
 */
@Serializable
@SerialName("PointPosterior")
data object PointPosterior : LinearPosterior<StochasticRegressionResult> {
    override fun sample(snapshot: StochasticRegressionResult, rng: Random, exploration: Double): F64VectorLike {
        if (exploration <= 0.0) return snapshot.weights
        return F64DenseVector.wrap(
            DoubleArray(snapshot.weights.size).also { destination ->
                sampleInto(snapshot, rng, destination, exploration)
            },
        )
    }

    override fun sampleInto(
        snapshot: StochasticRegressionResult,
        rng: Random,
        destination: DoubleArray,
        exploration: Double,
    ) {
        require(
            destination.size == snapshot.weights.size,
        ) { "destination size ${destination.size} must match weights size ${snapshot.weights.size}" }
        if (exploration <= 0.0) {
            for (i in destination.indices) destination[i] = snapshot.weights[i]
        } else {
            val sd = sqrt(exploration)
            for (i in destination.indices) destination[i] = rng.nextNormal(snapshot.weights[i], sd)
        }
    }

    /** Closes to `invMean(eta(x) + sd * ||x|| * N(0,1))` since the per-coord noise terms
     *  are iid; one Gaussian draw instead of one per coordinate. */
    override fun evaluate(
        snapshot: StochasticRegressionResult,
        x: F64VectorLike,
        rng: Random,
        exploration: Double,
        workspace: Workspace?,
    ): Double {
        val eta = snapshot.linearPredictor(x)
        if (exploration <= 0.0) return snapshot.link.invMean(eta)
        val xNormSq = x dot x
        return snapshot.link.invMean(eta + sqrt(exploration * xNormSq) * rng.nextNormal())
    }
}

/**
 * Per-coordinate Gaussian: each `w_i ~ N(weights[i], exploration / precision[i])`.
 * Cheap O(n) draws; ignores cross-feature correlations.
 */
@Serializable
@SerialName("FactorisedGaussian")
data object FactorisedGaussian : LinearPosterior<DiagonalRegressionResult> {
    override fun sample(snapshot: DiagonalRegressionResult, rng: Random, exploration: Double): F64VectorLike =
        F64DenseVector.wrap(
            DoubleArray(snapshot.weights.size).also { destination ->
                sampleInto(snapshot, rng, destination, exploration)
            },
        )

    override fun sampleInto(
        snapshot: DiagonalRegressionResult,
        rng: Random,
        destination: DoubleArray,
        exploration: Double,
    ) {
        require(
            destination.size == snapshot.weights.size,
        ) { "destination size ${destination.size} must match weights size ${snapshot.weights.size}" }
        for (i in destination.indices) {
            destination[i] = rng.nextNormal(
                snapshot.weights[i],
                sqrt(exploration / snapshot.precision[i]),
            )
        }
    }

    /** Sum of independent normals: `invMean(eta(x) + sqrt(exploration * Sum x_i^2 / precision[i]) * N(0,1))`. */
    override fun evaluate(
        snapshot: DiagonalRegressionResult,
        x: F64VectorLike,
        rng: Random,
        exploration: Double,
        workspace: Workspace?,
    ): Double {
        val eta = snapshot.linearPredictor(x)
        var variance = 0.0
        x.forEachStored { i, xi -> variance += xi * xi / snapshot.precision[i] }
        return snapshot.link.invMean(eta + sqrt(exploration * variance) * rng.nextNormal())
    }
}

/**
 * Posterior standard deviation at [x] under a unit exploration scale:
 * `sqrt(xT * Sigma * x) = ||L^-1 x||` for the precision factor `L` with `Sigma = (L * LT)^-1`.
 * One forward substitution, so the covariance is never formed.
 */
private fun PrecisionRegressionResult.predictiveDeviation(x: F64VectorLike, workspace: Workspace?): Double =
    workspace.borrow(featureSize) { v ->
        copy(x, F64DenseVector.wrap(v))
        precisionL.trsv(v, lower = true)
        F64DenseVector.wrap(v).norm2()
    }

/**
 * Full multivariate-Gaussian draw `w ~ N(weights, exploration * Sigma)` from the
 * precision factor `L` carried in the snapshot:
 *
 * `w = weights + sqrt(exploration) * L^-T * u` where `u ~ N(0, I)`.
 *
 * `L^-T` is a square root of `Sigma` because `L^-T * (L^-T)T = (L * LT)^-1`, so the draw
 * costs one back substitution: O(n^2), and no decomposition at draw time.
 */
@Serializable
@SerialName("MultivariateGaussian")
data object MultivariateGaussian : LinearPosterior<PrecisionRegressionResult> {
    override fun sample(snapshot: PrecisionRegressionResult, rng: Random, exploration: Double): F64VectorLike {
        val n = snapshot.weights.size
        val sd = sqrt(exploration)
        val u = DoubleArray(n) { rng.nextNormal(0.0, sd) }
        snapshot.precisionL.trsv(u, lower = true, transpose = true)
        return F64DenseVector.wrap(u).also { it.axpy(1.0, snapshot.weights) }
    }

    override fun sampleInto(
        snapshot: PrecisionRegressionResult,
        rng: Random,
        destination: DoubleArray,
        exploration: Double,
    ) {
        require(
            destination.size == snapshot.weights.size,
        ) { "destination size ${destination.size} must match weights size ${snapshot.weights.size}" }
        val sd = sqrt(exploration)
        for (i in destination.indices) destination[i] = rng.nextNormal(0.0, sd)
        snapshot.precisionL.trsv(destination, lower = true, transpose = true)
        for (i in destination.indices) destination[i] += snapshot.weights[i]
    }

    /** Closes to `invMean(eta(x) + sqrt(exploration) * ||L^-1 x|| * N(0,1))`; one triangular
     *  solve instead of sampling the full weight vector. */
    override fun evaluate(
        snapshot: PrecisionRegressionResult,
        x: F64VectorLike,
        rng: Random,
        exploration: Double,
        workspace: Workspace?,
    ): Double {
        val eta = snapshot.linearPredictor(x)
        val deviation = snapshot.predictiveDeviation(x, workspace)
        return snapshot.link.invMean(eta + sqrt(exploration) * deviation * rng.nextNormal())
    }
}

/**
 * LinUCB-style confidence-bound scoring: `invMean(eta(x) + exploration * sqrt(xT * Sigma * x))`.
 * Deterministic given the snapshot; no random draw at evaluate time; so the
 * `exploration` parameter here plays the role of LinUCB's `alpha` (confidence-bound
 * width), not the variance scale used by Thompson-style posteriors. [sample] returns
 * the snapshot's mean weights since UCB has no per-arm randomization; callers that
 * want sampled weights should pair with [MultivariateGaussian] instead.
 */
@Serializable
@SerialName("LinUcb")
data object LinUcb : LinearPosterior<PrecisionRegressionResult> {
    override fun sample(snapshot: PrecisionRegressionResult, rng: Random, exploration: Double): F64VectorLike =
        snapshot.weights

    override fun sampleInto(
        snapshot: PrecisionRegressionResult,
        rng: Random,
        destination: DoubleArray,
        exploration: Double,
    ) {
        require(
            destination.size == snapshot.weights.size,
        ) { "destination size ${destination.size} must match weights size ${snapshot.weights.size}" }
        for (i in destination.indices) destination[i] = snapshot.weights[i]
    }

    override fun evaluate(
        snapshot: PrecisionRegressionResult,
        x: F64VectorLike,
        rng: Random,
        exploration: Double,
        workspace: Workspace?,
    ): Double = snapshot.link.invMean(
        snapshot.linearPredictor(x) + exploration * snapshot.predictiveDeviation(x, workspace),
    )
}

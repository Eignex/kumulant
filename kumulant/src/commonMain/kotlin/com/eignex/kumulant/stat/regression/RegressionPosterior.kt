package com.eignex.kumulant.stat.regression

import com.eignex.koblas.F64VectorView
import com.eignex.kumulant.core.Result
import kotlin.random.Random

/**
 * Stateless scorer over a regression snapshot at a query point `x`. Generalises the
 * "score this arm under the current model and context" loop across linear regressors,
 * trees, and any future regressor type:
 *
 *  - For linear models the score is `bias + x . sample(weights)` (Thompson) or
 *    `bias + alpha * sqrt(xT * Sigma * x)` (UCB); see [com.eignex.kumulant.stat.regression.glm.LinearPosterior].
 *  - For tree models the snapshot is routed via `findLeaf(x)` and the leaf's
 *    weighted-variance summary drives the draw; see the `TreePosterior` family.
 *
 * Used by [com.eignex.kumulant.bandit.contextual.RegressionContextualBandit] and by Bayesian-
 * optimisation acquisition functions that need to score candidate points.
 */
interface RegressionPosterior<R : Result> {
    /**
     * Score a query point [x] under the regression [snapshot]. [exploration] controls
     * the posterior-variance scale (Thompson) or the UCB width (LinUcb-style);
     * `0.0` collapses to the point estimate.
     */
    fun evaluate(snapshot: R, x: F64VectorView, rng: Random, exploration: Double = 1.0): Double
}

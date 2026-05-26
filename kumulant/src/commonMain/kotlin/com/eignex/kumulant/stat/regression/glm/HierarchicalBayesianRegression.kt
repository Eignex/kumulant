package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.math.DenseMatrix
import com.eignex.kumulant.math.DenseVector

/**
 * Manager for a population of [BayesianRegressionStat] instances that share an
 * empirical-Bayes prior. New instances inherit the current [populationPrior]; periodic
 * [refit] re-fits the prior from the current per-instance posteriors. Cross-instance
 * transfer happens through that prior — older instances keep accumulating their own
 * state, but freshly created instances borrow from the population's collective
 * experience.
 *
 * Not thread-safe: callers should serialise [createInstance] / [untrack] / [refit]
 * externally if used concurrently. Individual instances inherit the [concurrency]
 * level chosen here for their own update path.
 *
 * Typical lifecycle:
 * ```
 * val pop = HierarchicalBayesianRegression(featureSize = 8)
 * val instanceA = pop.createInstance()  // seeded from initial isotropic prior
 * // ... feed observations to instanceA ...
 * val instanceB = pop.createInstance()  // still using initial prior
 * pop.refit()                           // population prior now reflects instanceA's evidence
 * val instanceC = pop.createInstance()  // benefits from instanceA's data via prior
 * ```
 */
class HierarchicalBayesianRegression(
    /** Feature dimensionality of every managed instance. */
    val featureSize: Int,
    /** Canonical GLM link propagated to every instance. */
    val link: Link = Link.Identity,
    /** Bias prior variance forwarded to each instance's constructor; not refitted. */
    val biasPriorVariance: Double = 1.0,
    /** Concurrency level forwarded to each instance. */
    val concurrency: Concurrency = Concurrency.None,
    initialPriorVariance: Double = 1.0,
    initialPriorMean: DenseVector? = null,
    initialPriorCovariance: DenseMatrix? = null,
) {
    init {
        require(featureSize > 0) { "featureSize must be positive" }
    }

    private val tracked = mutableListOf<BayesianRegressionStat>()

    /**
     * Population prior used to seed new instances. Initially isotropic
     * (`N(0, initialPriorVariance * I)`) or seeded from caller-supplied mean/covariance;
     * call [refit] to update it from the current per-instance posteriors.
     */
    var populationPrior: PopulationPrior = PopulationPrior(
        mean = initialPriorMean ?: DenseVector.zero(featureSize),
        covariance = initialPriorCovariance ?: DenseMatrix.diagonal(featureSize, initialPriorVariance),
        instanceCount = 0,
    )
        private set

    /** Allocate a fresh [BayesianRegressionStat] seeded with the current [populationPrior]. */
    fun createInstance(): BayesianRegressionStat {
        val inst = BayesianRegressionStat(
            featureSize = featureSize,
            priorVariance = biasPriorVariance,
            link = link,
            concurrency = concurrency,
            priorMean = populationPrior.mean,
            priorCovariance = populationPrior.covariance,
        )
        tracked += inst
        return inst
    }

    /** Refit [populationPrior] from the current per-instance posteriors. No-op if no
     *  instances are tracked. Existing instances keep their state — only future
     *  [createInstance] calls see the updated prior. */
    fun refit() {
        if (tracked.isEmpty()) return
        populationPrior = BayesianRegressionStat.fitPopulationPrior(tracked.map { it.read(0L) })
    }

    /** Remove [instance] from population tracking. Future [refit] calls will not see its
     *  posterior. Useful when an instance is retired or its data has gone stale. */
    fun untrack(instance: BayesianRegressionStat) {
        tracked.remove(instance)
    }

    /** Number of instances currently contributing to [refit]. */
    val instanceCount: Int get() = tracked.size
}

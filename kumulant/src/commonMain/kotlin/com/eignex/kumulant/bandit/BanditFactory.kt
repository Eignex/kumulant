@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.bandit

import com.eignex.kumulant.bandit.contextual.ContextualBanditSpec
import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.contextual.RegressionContextualSpec
import com.eignex.kumulant.bandit.univariate.Arm
import com.eignex.kumulant.bandit.univariate.BanditPolicy
import com.eignex.kumulant.bandit.univariate.BanditPolicySpec
import com.eignex.kumulant.bandit.univariate.BoltzmannBandit
import com.eignex.kumulant.bandit.univariate.BoltzmannSpec
import com.eignex.kumulant.bandit.univariate.EpsilonDecreasing
import com.eignex.kumulant.bandit.univariate.EpsilonDecreasingSpec
import com.eignex.kumulant.bandit.univariate.EpsilonGreedy
import com.eignex.kumulant.bandit.univariate.EpsilonGreedySpec
import com.eignex.kumulant.bandit.univariate.Exp3Bandit
import com.eignex.kumulant.bandit.univariate.Exp3Spec
import com.eignex.kumulant.bandit.univariate.Greedy
import com.eignex.kumulant.bandit.univariate.GreedySpec
import com.eignex.kumulant.bandit.univariate.KlUcb
import com.eignex.kumulant.bandit.univariate.KlUcbSpec
import com.eignex.kumulant.bandit.univariate.Moss
import com.eignex.kumulant.bandit.univariate.MossSpec
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.MultiArmedSpec
import com.eignex.kumulant.bandit.univariate.Posterior
import com.eignex.kumulant.bandit.univariate.RouletteWheelBandit
import com.eignex.kumulant.bandit.univariate.RouletteWheelSpec
import com.eignex.kumulant.bandit.univariate.ThompsonSampling
import com.eignex.kumulant.bandit.univariate.ThompsonSamplingSpec
import com.eignex.kumulant.bandit.univariate.TopTwoThompsonBandit
import com.eignex.kumulant.bandit.univariate.TopTwoThompsonSpec
import com.eignex.kumulant.bandit.univariate.UCB1
import com.eignex.kumulant.bandit.univariate.UCB1Normal
import com.eignex.kumulant.bandit.univariate.UCB1Tuned
import com.eignex.kumulant.bandit.univariate.Ucb1NormalSpec
import com.eignex.kumulant.bandit.univariate.Ucb1Spec
import com.eignex.kumulant.bandit.univariate.Ucb1TunedSpec
import com.eignex.kumulant.bandit.univariate.UcbV
import com.eignex.kumulant.bandit.univariate.UcbVSpec
import com.eignex.kumulant.bandit.univariate.UniformSelection
import com.eignex.kumulant.bandit.univariate.UniformSelectionSpec
import com.eignex.kumulant.bandit.univariate.UnivariateBanditSpec
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionResult
import com.eignex.kumulant.stat.regression.glm.FactorisedGaussian
import com.eignex.kumulant.stat.regression.glm.LinUcb
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import com.eignex.kumulant.stat.regression.glm.PointPosterior
import com.eignex.kumulant.stat.regression.glm.PrecisionRegressionResult
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionResult
import com.eignex.kumulant.stat.regression.tree.ForestPosterior
import com.eignex.kumulant.stat.regression.tree.ForestRegressionResult
import com.eignex.kumulant.stat.regression.tree.TreePosterior
import com.eignex.kumulant.stat.regression.tree.TreeRegressionResult
import kotlin.random.Random

/**
 * Build a live [BanditPolicy] from its spec.
 *
 * `Arm` and `Posterior` are themselves `@Serializable` sealed hierarchies, so
 * [ThompsonSamplingSpec] consumes them directly without an intermediate
 * factory hop.
 */
fun <R : Result> BanditPolicySpec<R>.materialize(): BanditPolicy<R> = when (this) {
    is ThompsonSamplingSpec<*> -> ThompsonSampling(arm as Arm<R>, posterior as Posterior<R>) as BanditPolicy<R>

    is Ucb1Spec -> UCB1(alpha, priorAlpha, priorBeta) as BanditPolicy<R>

    is Ucb1NormalSpec -> UCB1Normal(alpha, priorMean, priorWeight) as BanditPolicy<R>

    is Ucb1TunedSpec -> UCB1Tuned(alpha, priorMean, priorWeight) as BanditPolicy<R>

    is GreedySpec -> Greedy(priorMean, priorWeight, priorSquaredDeviations) as BanditPolicy<R>

    is EpsilonGreedySpec ->
        EpsilonGreedy(epsilon, priorMean, priorWeight, priorSquaredDeviations) as BanditPolicy<R>

    is EpsilonDecreasingSpec ->
        EpsilonDecreasing(epsilon, decay, priorMean, priorWeight, priorSquaredDeviations) as BanditPolicy<R>

    is UniformSelectionSpec ->
        UniformSelection(priorMean, priorWeight, priorSquaredDeviations) as BanditPolicy<R>

    is KlUcbSpec -> KlUcb(c, tolerance, priorAlpha, priorBeta) as BanditPolicy<R>

    is MossSpec -> Moss(nbrArms, priorMean, priorWeight) as BanditPolicy<R>

    is UcbVSpec -> UcbV(zeta, c, priorMean, priorWeight) as BanditPolicy<R>
}

/** Build a live [UnivariateBandit] from its spec. */
fun MultiArmedSpec<*>.materialize(random: Random = Random.Default): MultiArmedBandit<Result> {
    val moss = policy as? MossSpec
    require(moss == null || moss.nbrArms == nbrArms) {
        "MossSpec.nbrArms (${moss?.nbrArms}) must match MultiArmedSpec.nbrArms ($nbrArms)"
    }
    return MultiArmedBandit(
        nbrArms = nbrArms,
        policy = policy.materialize() as BanditPolicy<Result>,
        random = random,
    )
}

/** Build a live [RouletteWheelBandit] from its spec. */
fun RouletteWheelSpec.materialize(random: Random = Random.Default): RouletteWheelBandit =
    RouletteWheelBandit(nbrArms, reactionFactor, segmentLength, initialWeight, minWeight, random)

/** Build a live [BoltzmannBandit] from its spec. */
fun BoltzmannSpec.materialize(random: Random = Random.Default): BoltzmannBandit =
    BoltzmannBandit(nbrArms, priorMean, priorWeight, initialTau, minTau, decay, random)

/** Build a live [Exp3Bandit] from its spec, resolving null `eta` / `gamma` to defaults. */
fun Exp3Spec.materialize(random: Random = Random.Default): Exp3Bandit {
    val resolvedEta = eta ?: Exp3Bandit.defaultEta(nbrArms)
    // Resolve through defaultGamma so a spec-built bandit tunes identically to a directly-built one.
    val resolvedGamma = gamma ?: Exp3Bandit.defaultGamma(resolvedEta)
    return Exp3Bandit(nbrArms, resolvedEta, resolvedGamma, random)
}

/** Build a live [TopTwoThompsonBandit] from its spec. */
fun <R : Result> TopTwoThompsonSpec<R>.materialize(random: Random = Random.Default): TopTwoThompsonBandit<R> =
    TopTwoThompsonBandit(
        nbrArms = nbrArms,
        policy = (policy as BanditPolicySpec<R>).materialize() as ThompsonSampling<R>,
        beta = beta,
        maxResamples = maxResamples,
        random = random,
    )

/** Dispatch any [UnivariateBanditSpec] to its concrete bandit. */
fun UnivariateBanditSpec.materialize(random: Random = Random.Default): Bandit = when (this) {
    is MultiArmedSpec<*> -> materialize(random)
    is RouletteWheelSpec -> materialize(random)
    is BoltzmannSpec -> materialize(random)
    is Exp3Spec -> materialize(random)
    is TopTwoThompsonSpec<*> -> (this as TopTwoThompsonSpec<Result>).materialize(random)
}

/** Build a live [RegressionContextualBandit] from its composed regression specs. */
fun <R : Result> RegressionContextualSpec<R>.materialize(
    random: Random = Random.Default,
    concurrency: Concurrency = Concurrency.None,
): RegressionContextualBandit<R> {
    val template = regression.materialize(concurrency)
    val global = globalRegression?.materialize(concurrency)
    posterior.requireCompatible(template.read(0L), "regression")
    if (global != null) {
        posterior.requireCompatible(global.read(0L), "globalRegression")
        require(global.featureSize == template.featureSize) {
            "globalRegression.featureSize (${global.featureSize}) must match " +
                "regression.featureSize (${template.featureSize})"
        }
    }
    return RegressionContextualBandit(
        nbrArms = nbrArms,
        template = template,
        posterior = posterior,
        exploration = exploration,
        globalTemplate = global,
        random = random,
    )
}

private fun RegressionPosterior<*>.requireCompatible(snapshot: Result, path: String) {
    val compatible = when (this) {
        PointPosterior -> snapshot is StochasticRegressionResult
        FactorisedGaussian -> snapshot is DiagonalRegressionResult
        MultivariateGaussian, LinUcb -> snapshot is PrecisionRegressionResult
        is TreePosterior -> snapshot is TreeRegressionResult
        is ForestPosterior -> snapshot is ForestRegressionResult
        else -> true
    }
    require(compatible) { "$path: ${snapshot::class.simpleName} is incompatible with ${this::class.simpleName}" }
}

/** Dispatch any [ContextualBanditSpec] to its concrete bandit. */
fun ContextualBanditSpec.materialize(
    random: Random = Random.Default,
    concurrency: Concurrency = Concurrency.None,
): Bandit = when (this) {
    is RegressionContextualSpec<*> -> materialize(random, concurrency)
}

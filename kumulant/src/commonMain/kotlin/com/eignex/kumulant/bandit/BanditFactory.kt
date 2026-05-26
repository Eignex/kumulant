@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.bandit

import com.eignex.kumulant.bandit.contextual.ContextualBanditSpec
import com.eignex.kumulant.bandit.contextual.KnnContextualBandit
import com.eignex.kumulant.bandit.contextual.KnnContextualSpec
import com.eignex.kumulant.bandit.contextual.LinearRegressionSpec
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
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.schema.Sgd
import com.eignex.kumulant.stat.regression.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.ConstantRate
import com.eignex.kumulant.stat.regression.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.LinearRegressionResult
import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.regression.StochasticRegressionStat
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
fun MultiArmedSpec<*>.materialize(random: Random = Random.Default): MultiArmedBandit<Result> = MultiArmedBandit(
    nbrArms = nbrArms,
    policy = policy.materialize() as BanditPolicy<Result>,
    random = random,
)

/** Build a live [RouletteWheelBandit] from its spec. */
fun RouletteWheelSpec.materialize(random: Random = Random.Default): RouletteWheelBandit =
    RouletteWheelBandit(nbrArms, reactionFactor, segmentLength, initialWeight, minWeight, random)

/** Build a live [BoltzmannBandit] from its spec. */
fun BoltzmannSpec.materialize(random: Random = Random.Default): BoltzmannBandit =
    BoltzmannBandit(nbrArms, priorMean, priorWeight, initialTau, minTau, decay, random)

/** Build a live [Exp3Bandit] from its spec, resolving null `eta` / `gamma` to defaults. */
fun Exp3Spec.materialize(random: Random = Random.Default): Exp3Bandit {
    val resolvedEta = eta ?: Exp3Bandit.defaultEta(nbrArms)
    val resolvedGamma = gamma ?: (nbrArms * resolvedEta).coerceAtMost(1.0)
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

/** Build a live linear [RegressionStat] from its spec. */
private fun LinearRegressionSpec.materialize(concurrency: Concurrency): RegressionStat<out LinearRegressionResult> =
    when (this) {
        is LinearRegressionSpec.Bayesian -> BayesianRegressionStat(
            featureSize = featureSize,
            priorVariance = priorVariance,
            concurrency = concurrency,
        )

        is LinearRegressionSpec.Diagonal -> DiagonalRegressionStat(
            featureSize = featureSize,
            priorPrecision = priorPrecision,
            learningRate = ConstantRate(learningRate),
            concurrency = concurrency,
        )

        is LinearRegressionSpec.Stochastic -> StochasticRegressionStat(
            featureSize = featureSize,
            optimizer = Sgd(ConstantRate(learningRate)),
            concurrency = concurrency,
        )
    }

/** Build a live [RegressionContextualBandit] from its spec. */
fun RegressionContextualSpec.materialize(
    random: Random = Random.Default,
    concurrency: Concurrency = Concurrency.None,
): RegressionContextualBandit<out LinearRegressionResult> {
    val template = regression.materialize(concurrency)
    val global = globalRegression?.materialize(concurrency)
    @Suppress("UNCHECKED_CAST")
    return RegressionContextualBandit(
        nbrArms = nbrArms,
        template = template as RegressionStat<LinearRegressionResult>,
        posterior = posterior as RegressionPosterior<LinearRegressionResult>,
        exploration = exploration,
        globalTemplate = global as? RegressionStat<LinearRegressionResult>,
        random = random,
    )
}

/** Built-in distance functions referenced by [KnnContextualSpec.distance]. Extend
 *  by passing a custom map when constructing the bandit programmatically. */
val knnDistanceRegistry: Map<String, (VectorView, VectorView) -> Double> = mapOf(
    "squaredL2" to KnnContextualBandit.Companion::squaredL2,
)

/** Build a live [KnnContextualBandit] from its spec, resolving the distance
 *  function via [distanceRegistry] (defaults to [knnDistanceRegistry]). */
fun KnnContextualSpec.materialize(
    random: Random = Random.Default,
    distanceRegistry: Map<String, (VectorView, VectorView) -> Double> = knnDistanceRegistry,
): KnnContextualBandit {
    val dist = distanceRegistry[distance]
        ?: error("Unknown KnnContextualSpec.distance '$distance'. Known: ${distanceRegistry.keys}")
    return KnnContextualBandit(
        nbrArms = nbrArms,
        k = k,
        maxHistoryPerArm = maxHistoryPerArm,
        coldStartScore = coldStartScore,
        exploration = exploration,
        distance = dist,
        random = random,
    )
}

/** Dispatch any [ContextualBanditSpec] to its concrete bandit. */
fun ContextualBanditSpec.materialize(
    random: Random = Random.Default,
    concurrency: Concurrency = Concurrency.None,
): Bandit = when (this) {
    is RegressionContextualSpec -> materialize(random, concurrency)
    is KnnContextualSpec -> materialize(random)
}

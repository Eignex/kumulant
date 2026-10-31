package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.TrackedUnivariateBandit
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.serialization.UnivariateBanditSpecSerializer
import com.eignex.kumulant.schema.spec.PairedStatSpec
import com.eignex.kumulant.schema.spec.SeriesStatSpec
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable specification for a univariate bandit instance.
 *
 * Each variant mirrors the constructor of its concrete class. Materialisation
 * lives in `BanditFactory`, which threads a
 * `Random` source through at build time (the source itself is not part of the
 * wire format).
 */
@Serializable(with = UnivariateBanditSpecSerializer::class)
sealed interface UnivariateBanditSpec

/** Spec for [MultiArmedBandit]. */
@Serializable
@SerialName("MultiArmed")
data class MultiArmedSpec<R : Result>(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Selection rule owning per-arm cumulators. */
    val policy: BanditPolicySpec<R>,
) : UnivariateBanditSpec

/** Spec for [RouletteWheelBandit]. */
@Serializable
@SerialName("RouletteWheel")
data class RouletteWheelSpec(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Blend factor for the Ropke-Pisinger weight update. */
    val reactionFactor: Double = 0.1,
    /** Updates between successive weight rebalances. */
    val segmentLength: Int = 10,
    /** Starting weight assigned to every arm. */
    val initialWeight: Double = 1.0,
    /** Floor on the rebalanced weight. */
    val minWeight: Double = 0.01,
) : UnivariateBanditSpec

/** Spec for [Exp3Bandit]. Pass `null` for [eta] / [gamma] to use the algorithm's defaults. */
@Serializable
@SerialName("Exp3")
data class Exp3Spec(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Learning rate on per-arm gain updates; `null` selects `sqrt(ln(K)/K)`. */
    val eta: Double? = null,
    /** Exploration mix probability; `null` selects `min(eta, 1)`. */
    val gamma: Double? = null,
) : UnivariateBanditSpec

/** Spec for [TopTwoThompsonBandit]. */
@Serializable
@SerialName("TopTwoThompson")
data class TopTwoThompsonSpec<R : Result>(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Thompson sampling policy carrying the per-arm posterior. */
    val policy: ThompsonSamplingSpec<R>,
    /** Probability of playing the round's top sample. */
    val beta: Double = 0.5,
    /** Cap on the resample loop when searching for the second arm. */
    val maxResamples: Int = 32,
) : UnivariateBanditSpec

/** Spec for [TrackedUnivariateBandit] and its optional observation accumulators. */
@Serializable
@SerialName("TrackedUnivariate")
data class TrackedUnivariateSpec(
    /** Bandit whose decisions and rewards are observed. */
    val inner: UnivariateBanditSpec,
    /** Accumulator observing chosen arm indices. */
    val chooseTemplate: SeriesStatSpec<*>? = null,
    /** Accumulator observing arm-index/reward pairs. */
    val updateArmRewardTemplate: PairedStatSpec<*>? = null,
) : UnivariateBanditSpec

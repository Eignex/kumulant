package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable specification for a [BanditPolicy].
 *
 * Each variant `@SerialName`s to its policy's class name so a JSON `$type`
 * payload mirrors what a Kotlin reader would type. Construction lives in
 * [com.eignex.kumulant.bandit.BanditFactory].
 *
 * Specs that take an [Arm] or [Posterior] consume them directly — those
 * hierarchies are already sealed-and-`@Serializable` so they round-trip on the
 * wire alongside the policy.
 */
@Serializable
sealed interface BanditPolicySpec<R : Result>

/** Spec for [ThompsonSampling]. */
@Serializable
@SerialName("ThompsonSampling")
data class ThompsonSamplingSpec<R : Result>(
    val arm: Arm<R>,
    val posterior: Posterior<R>,
) : BanditPolicySpec<R>

/** Spec for [UCB1]. */
@Serializable
@SerialName("UCB1")
data class Ucb1Spec(
    val alpha: Double = 1.0,
    val priorAlpha: Double = 1.0,
    val priorBeta: Double = 1.0,
) : BanditPolicySpec<BernoulliSumResult>

/** Spec for [UCB1Normal]. */
@Serializable
@SerialName("UCB1Normal")
data class Ucb1NormalSpec(
    val alpha: Double = 1.0,
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<MomentsResult>

/** Spec for [UCB1Tuned]. */
@Serializable
@SerialName("UCB1Tuned")
data class Ucb1TunedSpec(
    val alpha: Double = 1.0,
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<MomentsResult>

/** Spec for [Greedy]. */
@Serializable
@SerialName("Greedy")
data class GreedySpec(
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
    val priorSquaredDeviations: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [EpsilonGreedy]. */
@Serializable
@SerialName("EpsilonGreedy")
data class EpsilonGreedySpec(
    val epsilon: Double = 0.1,
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
    val priorSquaredDeviations: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [EpsilonDecreasing]. */
@Serializable
@SerialName("EpsilonDecreasing")
data class EpsilonDecreasingSpec(
    val epsilon: Double = 2.0,
    val decay: Double = 0.5,
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
    val priorSquaredDeviations: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [UniformSelection]. */
@Serializable
@SerialName("UniformSelection")
data class UniformSelectionSpec(
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
    val priorSquaredDeviations: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [KlUcb]. */
@Serializable
@SerialName("KlUcb")
data class KlUcbSpec(
    val c: Double = 0.0,
    val tolerance: Double = 1e-6,
    val priorAlpha: Double = 1.0,
    val priorBeta: Double = 1.0,
) : BanditPolicySpec<BernoulliSumResult>

/** Spec for [Moss]. */
@Serializable
@SerialName("Moss")
data class MossSpec(
    val nbrArms: Int,
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<WeightedMeanResult>

/** Spec for [UcbV]. */
@Serializable
@SerialName("UcbV")
data class UcbVSpec(
    val zeta: Double = 1.2,
    val c: Double = 1.0,
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<MomentsResult>

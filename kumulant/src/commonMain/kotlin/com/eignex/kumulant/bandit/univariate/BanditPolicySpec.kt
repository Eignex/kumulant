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
 * `BanditFactory`.
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
    /** Per-arm prior + value encoding for the sampler. */
    val arm: Arm<R>,
    /** Sampler that turns a per-arm snapshot into a score. */
    val posterior: Posterior<R>,
) : BanditPolicySpec<R>

/** Spec for [UCB1]. */
@Serializable
@SerialName("UCB1")
data class Ucb1Spec(
    /** Exploration scale on the confidence-bound term. */
    val alpha: Double = 1.0,
    /** Beta-prior shape `alpha`. */
    val priorAlpha: Double = 1.0,
    /** Beta-prior shape `beta`. */
    val priorBeta: Double = 1.0,
) : BanditPolicySpec<BernoulliSumResult>

/** Spec for [UCB1Normal]. */
@Serializable
@SerialName("UCB1Normal")
data class Ucb1NormalSpec(
    /** Exploration scale on the confidence-bound term. */
    val alpha: Double = 1.0,
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<MomentsResult>

/** Spec for [UCB1Tuned]. */
@Serializable
@SerialName("UCB1Tuned")
data class Ucb1TunedSpec(
    /** Exploration scale on the confidence-bound term. */
    val alpha: Double = 1.0,
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<MomentsResult>

/** Spec for [Greedy]. */
@Serializable
@SerialName("Greedy")
data class GreedySpec(
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
    /** Prior on `Sum (x - mean)^2 * w`. */
    val priorSquaredDeviations: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [EpsilonGreedy]. */
@Serializable
@SerialName("EpsilonGreedy")
data class EpsilonGreedySpec(
    /** Probability of exploring uniformly. */
    val epsilon: Double = 0.1,
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
    /** Prior on `Sum (x - mean)^2 * w`. */
    val priorSquaredDeviations: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [EpsilonDecreasing]. */
@Serializable
@SerialName("EpsilonDecreasing")
data class EpsilonDecreasingSpec(
    /** Initial exploration scale. */
    val epsilon: Double = 2.0,
    /** Decay exponent applied to the running sample count. */
    val decay: Double = 0.5,
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
    /** Prior on `Sum (x - mean)^2 * w`. */
    val priorSquaredDeviations: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [UniformSelection]. */
@Serializable
@SerialName("UniformSelection")
data class UniformSelectionSpec(
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
    /** Prior on `Sum (x - mean)^2 * w`. */
    val priorSquaredDeviations: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [KlUcb]. */
@Serializable
@SerialName("KlUcb")
data class KlUcbSpec(
    /** Confidence padding term coefficient. */
    val c: Double = 0.0,
    /** Binary-search tolerance for the quantile root. */
    val tolerance: Double = 1e-6,
    /** Beta-prior shape `alpha`. */
    val priorAlpha: Double = 1.0,
    /** Beta-prior shape `beta`. */
    val priorBeta: Double = 1.0,
) : BanditPolicySpec<BernoulliSumResult>

/** Spec for [Moss]. */
@Serializable
@SerialName("Moss")
data class MossSpec(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<WeightedMeanResult>

/** Spec for [UcbV]. */
@Serializable
@SerialName("UcbV")
data class UcbVSpec(
    /** Variance-term scale. */
    val zeta: Double = 1.2,
    /** Bias-correction term scale. */
    val c: Double = 1.0,
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<MomentsResult>

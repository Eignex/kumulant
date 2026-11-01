package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.serialization.BanditPolicySpecSerializer
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable specification for a [BanditPolicy].
 *
 * JSON uses names for default policies, such as `"ucb1"`, and operator keys for configured policies.
 * Other formats retain the generated class tags. Construction lives in `BanditFactory`.
 *
 * Specs that take an [Arm] or [Posterior] consume them directly; those
 * hierarchies are already sealed-and-`@Serializable` so they round-trip on the
 * wire alongside the policy.
 */
@Serializable(with = BanditPolicySpecSerializer::class)
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

/** Spec for [EpsilonGreedy]. */
@Serializable
@SerialName("EpsilonGreedy")
data class EpsilonGreedySpec(
    /** Exploration probability at `decay = 0`; otherwise the scale of the annealed probability. */
    val epsilon: Double = 0.1,
    /** Decay exponent applied to the total sample count; `0.0` keeps exploration fixed. */
    val decay: Double = 0.0,
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<WeightedVarianceResult>

/** Spec for [Boltzmann]. */
@Serializable
@SerialName("Boltzmann")
data class BoltzmannSpec(
    /** Initial softmax temperature. */
    val initialTau: Double = 1.0,
    /** Floor on the temperature schedule. */
    val minTau: Double = 1e-3,
    /** Cooling decay exponent: `tau(t) = initialTau / t^decay`. */
    val decay: Double = 1.0,
    /** Per-arm prior on the running reward mean. */
    val priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    val priorWeight: Double = 0.02,
) : BanditPolicySpec<WeightedMeanResult>

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
) : BanditPolicySpec<WeightedVarianceResult>

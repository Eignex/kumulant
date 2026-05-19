package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable specification for a univariate bandit instance.
 *
 * Each variant mirrors the constructor of its concrete class. Materialisation
 * lives in [com.eignex.kumulant.bandit.BanditFactory], which threads a
 * `Random` source through at build time (the source itself is not part of the
 * wire format).
 */
@Serializable
sealed interface UnivariateBanditSpec

/** Spec for [MultiArmedBandit]. */
@Serializable
@SerialName("MultiArmed")
data class MultiArmedSpec<R : Result>(
    val nbrArms: Int,
    val policy: BanditPolicySpec<R>,
) : UnivariateBanditSpec

/** Spec for [RouletteWheelBandit]. */
@Serializable
@SerialName("RouletteWheel")
data class RouletteWheelSpec(
    val nbrArms: Int,
    val reactionFactor: Double = 0.1,
    val segmentLength: Int = 10,
    val initialWeight: Double = 1.0,
    val minWeight: Double = 0.01,
) : UnivariateBanditSpec

/** Spec for [BoltzmannBandit]. */
@Serializable
@SerialName("Boltzmann")
data class BoltzmannSpec(
    val nbrArms: Int,
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
    val initialTau: Double = 1.0,
    val minTau: Double = 1e-3,
    val decay: Double = 1.0,
) : UnivariateBanditSpec

/** Spec for [Exp3Bandit]. */
@Serializable
@SerialName("Exp3")
data class Exp3Spec(
    val nbrArms: Int,
    val eta: Double? = null,
    val gamma: Double? = null,
) : UnivariateBanditSpec

/** Spec for [TopTwoThompsonBandit]. */
@Serializable
@SerialName("TopTwoThompson")
data class TopTwoThompsonSpec<R : Result>(
    val nbrArms: Int,
    val policy: ThompsonSamplingSpec<R>,
    val beta: Double = 0.5,
    val maxResamples: Int = 32,
) : UnivariateBanditSpec

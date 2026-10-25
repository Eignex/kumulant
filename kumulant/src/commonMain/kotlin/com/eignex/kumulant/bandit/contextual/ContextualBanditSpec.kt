package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.bandit.TrackedContextualBandit
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.serialization.ContextualBanditSpecSerializer
import com.eignex.kumulant.schema.spec.PairedStatSpec
import com.eignex.kumulant.schema.spec.RegressionStatSpec
import com.eignex.kumulant.stat.regression.RegressionPosterior
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable specification for a contextual bandit instance.
 *
 * Materialisation lives in `BanditFactory`.
 */
@Serializable(with = ContextualBanditSpecSerializer::class)
sealed interface ContextualBanditSpec

/**
 * Spec for [RegressionContextualBandit] using the same [RegressionStatSpec] trees as statistical schemas.
 * [R] ties the per-arm and optional global regressors to their [posterior] scoring rule.
 * Built-in linear, tree, and forest posteriors are wire-portable.
 */
@Serializable
@SerialName("RegressionContextual")
data class RegressionContextualSpec<R : Result>(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Per-arm regressor template; cloned for each arm. */
    val regression: RegressionStatSpec<R>,
    /** Stateless arm scorer applied to each per-arm snapshot. */
    val posterior: RegressionPosterior<R>,
    /** Per-evaluate exploration scale forwarded to [posterior]. */
    val exploration: Double = 1.0,
    /** Template for the global pooling regressor; `null` disables pooling. */
    val globalRegression: RegressionStatSpec<R>? = null,
) : ContextualBanditSpec

/** Spec for [TrackedContextualBandit] and its optional observation accumulators. */
@Serializable
@SerialName("TrackedContextual")
data class TrackedContextualSpec(
    /** Bandit whose decisions and rewards are observed. */
    val inner: ContextualBanditSpec,
    /** Width of the context supplied to the bandit. */
    val contextFeatureSize: Int,
    /** Accumulator observing context/chosen-arm pairs. */
    val chooseTemplate: RegressionStatSpec<*>? = null,
    /** Accumulator observing contexts prefixed with the chosen arm and reward targets. */
    val updateJointTemplate: RegressionStatSpec<*>? = null,
    /** Accumulator observing context/reward pairs. */
    val updateMarginalTemplate: RegressionStatSpec<*>? = null,
    /** Accumulator observing arm-index/reward pairs. */
    val updateArmRewardTemplate: PairedStatSpec<*>? = null,
) : ContextualBanditSpec

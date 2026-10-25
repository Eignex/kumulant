package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.serialization.ContextualBanditSpecSerializer
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

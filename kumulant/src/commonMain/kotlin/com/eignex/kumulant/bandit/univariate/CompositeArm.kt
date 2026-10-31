package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.operation.FilterSeriesStat
import com.eignex.kumulant.operation.TransformValueStat
import com.eignex.kumulant.operation.weightBy
import com.eignex.kumulant.schema.expr.BoolExpr
import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.requireBound
import com.eignex.kumulant.schema.runtime.ListStats
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Composite [Arm] built from N independent sub-arms. Each observation is routed
 * through every sub-arm's [CompositeSubArm.valueExpr] / [CompositeSubArm.weightExpr] /
 * [CompositeSubArm.filter] AST before being fed to the corresponding per-sub-arm
 * accumulator. The composite result is a [ResultList] of the sub-snapshots; pair
 * with [CompositePosterior] to combine sub-arm draws into a single score.
 *
 * Plumbing is AST-driven (no lambdas) so the whole spec is wire-serializable.
 * Per-sub-arm `encode` is still applied after `valueExpr`; e.g. with `LogNormalArm`
 * + `valueExpr = X` the lognormal's own `encode = ln` runs, so don't double-log via
 * `valueExpr = X.ln()`.
 *
 * Example; zero-inflated lognormal revenue:
 * ```
 * val ziln = CompositeArm(listOf(
 *     CompositeSubArm(BernoulliArm(), valueExpr = ifThenElse(X gt 0.0, 1.0, 0.0)),
 *     CompositeSubArm(LogNormalArm(), filter = X gt 0.0),
 * ))
 * val score = CompositePosterior(
 *     subPosteriors = listOf(BetaPosterior, LogNormalGammaPosterior),
 *     combine = V(0) * V(1),
 * )
 * val bandit = MultiArmedBandit(nbrArms = 4, policy = ThompsonSampling(ziln, score))
 * ```
 */
@Serializable
@SerialName("CompositeArm")
data class CompositeArm(
    /** Sub-arms whose stats receive routed observations. */
    val subArms: List<CompositeSubArm>,
) : Arm<ResultList<Result>> {
    init {
        require(subArms.isNotEmpty()) { "CompositeArm requires at least one subArm" }
    }

    override fun createStat(): SeriesStat<ResultList<Result>> = compose()

    /** Companion host for factory helpers. */
    companion object
}

/**
 * One leg of a [CompositeArm]: which arm receives observations, with optional
 * AST-driven transformation of value, weight, and a filter predicate.
 */
@Serializable
@SerialName("CompositeSubArm")
data class CompositeSubArm(
    /** Sub-arm spec; receives routed observations. */
    val arm: Arm<*>,
    /** Expression evaluated against the raw observation to produce this sub-arm's input
     *  value. Default `X` passes the observation through unchanged (per-arm `encode`
     *  still applies). */
    val valueExpr: ScalarExpr = X,
    /** Multiplier on the observation's weight; defaults to `1.0` (passthrough). */
    val weightExpr: ScalarExpr = Const(1.0),
    /** If non-null, this sub-arm only sees observations where the predicate evaluates
     *  true against the raw value. */
    val filter: BoolExpr? = null,
)

private fun CompositeArm.compose(): SeriesStat<ResultList<Result>> = ListStats(
    subArms.mapIndexed { index, sub -> index.toString() to sub.routedStat() },
)

@Suppress("UNCHECKED_CAST")
private fun CompositeSubArm.routedStat(): SeriesStat<Result> {
    valueExpr.requireBound("valueExpr")
    weightExpr.requireBound("weightExpr")
    filter?.requireBound("filter")
    val accumulator = arm.createStat() as SeriesStat<Result>
    val transformed = TransformValueStat(accumulator) { arm.encode(valueExpr.eval(it)) }
    val weighted = transformed.weightBy { weightExpr.eval(it) }
    return filter?.let { predicate -> FilterSeriesStat(weighted) { predicate.eval(it) } } ?: weighted
}

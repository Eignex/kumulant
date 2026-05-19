package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.schema.BoolExpr
import com.eignex.kumulant.schema.Const
import com.eignex.kumulant.schema.ScalarExpr
import com.eignex.kumulant.schema.X
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
 * Per-sub-arm `encode` is still applied after `valueExpr` — e.g. with `LogNormalArm`
 * + `valueExpr = X` the lognormal's own `encode = ln` runs, so don't double-log via
 * `valueExpr = Log(X)`.
 *
 * Example — zero-inflated lognormal revenue:
 * ```
 * val ziln = CompositeArm(listOf(
 *     CompositeSubArm(BernoulliArm(), valueExpr = IfExpr(X gt 0.0, Const(1.0), Const(0.0))),
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
    init { require(subArms.isNotEmpty()) { "CompositeArm requires at least one subArm" } }

    override fun createStat(): SeriesStat<ResultList<Result>> =
        CompositeStat(subArms, subArms.map { it.arm.createStat() })

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

/** Live composite accumulator: fans each observation through the per-sub-arm AST. */
internal class CompositeStat(
    private val subArms: List<CompositeSubArm>,
    initialSubStats: List<SeriesStat<*>>,
) : SeriesStat<ResultList<Result>> {

    private val subStats: Array<SeriesStat<*>> = initialSubStats.toTypedArray()

    override val concurrency: Concurrency = subStats.firstOrNull()?.concurrency ?: Concurrency.None

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        for (i in subArms.indices) {
            val sub = subArms[i]
            if (sub.filter?.eval(value) == false) continue
            val routed = sub.valueExpr.eval(value)

            @Suppress("UNCHECKED_CAST")
            val encoded = (sub.arm as Arm<Result>).encode(routed)
            val w = weight * sub.weightExpr.eval(value)
            @Suppress("UNCHECKED_CAST")
            (subStats[i] as SeriesStat<Result>).update(encoded, timestampNanos, w)
        }
    }

    override fun read(timestampNanos: Long): ResultList<Result> =
        ResultList(subStats.map { it.read(timestampNanos) })

    override fun merge(values: ResultList<Result>) {
        require(values.results.size == subStats.size) {
            "merge: results.size=${values.results.size} does not match subStats.size=${subStats.size}"
        }
        for (i in subStats.indices) {
            @Suppress("UNCHECKED_CAST")
            (subStats[i] as SeriesStat<Result>).merge(values.results[i])
        }
    }

    /** Reset re-seeds the prior pseudo-counts by rebuilding sub-stats via [Arm.createStat],
     *  matching the priors-restored semantics of other Stat resets in the library. */
    override fun reset() {
        for (i in subArms.indices) subStats[i] = subArms[i].arm.createStat()
    }

    override fun create(concurrency: Concurrency?): SeriesStat<ResultList<Result>> =
        CompositeStat(subArms, subStats.map { it.create(concurrency) })
}

package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.schema.ScalarExpr
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.random.Random

/**
 * Composite [Posterior] over the sub-snapshots produced by a [CompositeArm]. Each
 * sub-posterior draws independently; the resulting samples are packed as `V(0)..V(N-1)`
 * and reduced to a single score by the [combine] AST.
 *
 * Wire-serializable because every leg is either a serialisable [Posterior] or a
 * serialisable [ScalarExpr]. For zero-inflated lognormal revenue:
 * ```
 * CompositePosterior(
 *     subPosteriors = listOf(BetaPosterior, LogNormalGammaPosterior),
 *     combine = V(0) * V(1),  // P(positive) * positive_value
 * )
 * ```
 *
 * Combiners are sample-based, not summary-based — they see one draw per sub-posterior,
 * not parameters. Combiners that need raw posterior moments (e.g. `exp(mu + sigma^2/2)`
 * for the lognormal *mean* rather than a draw) fall outside this surface and want a
 * bespoke posterior.
 */
@Serializable
@SerialName("CompositePosterior")
data class CompositePosterior(
    /** Per-sub-arm posteriors, parallel to [CompositeArm.subArms]. */
    val subPosteriors: List<Posterior<*>>,
    /** Score expression evaluated against the sub-arm draws (`V(0)..V(N-1)`). */
    val combine: ScalarExpr,
) : Posterior<ResultList<Result>> {

    init { require(subPosteriors.isNotEmpty()) { "CompositePosterior requires at least one subPosterior" } }

    override fun sample(snapshot: ResultList<Result>, rng: Random): Double {
        require(snapshot.results.size == subPosteriors.size) {
            "snapshot results.size=${snapshot.results.size} does not match subPosteriors.size=${subPosteriors.size}"
        }
        val draws = DoubleArray(subPosteriors.size) { i ->
            @Suppress("UNCHECKED_CAST")
            (subPosteriors[i] as Posterior<Result>).sample(snapshot.results[i], rng)
        }
        return combine.eval(0.0, 0.0, draws)
    }
}

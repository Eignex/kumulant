package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Comparison used by [RatioVsTargetStat] to decide whether an observation matches the target. */
@Serializable
enum class TargetComparison {
    /** Match when `value > threshold`. */
    Above,

    /** Match when `value >= threshold`. */
    AtLeast,

    /** Match when `value < threshold`. */
    Below,

    /** Match when `value <= threshold`. */
    AtMost,

    /** Match when `value == threshold`. */
    Equals,
}

/** Fraction of observations matching the configured target. */
@Serializable
@SerialName("RatioResult")
data class RatioResult(
    /** Threshold the values were compared against. */
    val threshold: Double,
    /** Comparison strategy applied at each update. */
    val comparison: TargetComparison,
    /** Total weight of observations that matched the target. */
    val matched: Double,
    /** Total weight of observations seen. */
    val total: Double,
) : Result {
    /** Matched / total; `NaN` when no observations have been recorded. */
    val ratio: Double get() = if (total == 0.0) Double.NaN else matched / total
}

/**
 * Cumulative weighted fraction of observations meeting a target. Wrap with `.windowed(...)`
 * to constrain the ratio to a sliding wall-clock window (the standard way of getting
 * "fraction matching over the last N seconds").
 *
 * **Use cases:** SLO compliance ratios, error budgets, "fraction over threshold"
 * health metrics. Compose with `.transform(...)` upstream to project arbitrary
 * predicates onto the value before it reaches the stat.
 *
 * **Memory:** O(1) — two cells.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Per-cell additive atomics (category 1). Concurrent updates may
 * drift slightly under contention but never throw.
 */
class RatioVsTargetStat(
    /** Target threshold compared against each value. */
    val threshold: Double,
    /** Comparison strategy; defaults to [TargetComparison.AtLeast]. */
    val comparison: TargetComparison = TargetComparison.AtLeast,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<RatioResult> {

    private val streamMode = concurrency.additiveMode()
    private val matched = streamMode.newDouble(0.0)
    private val total = streamMode.newDouble(0.0)

    private fun matches(value: Double): Boolean = when (comparison) {
        TargetComparison.Above -> value > threshold
        TargetComparison.AtLeast -> value >= threshold
        TargetComparison.Below -> value < threshold
        TargetComparison.AtMost -> value <= threshold
        TargetComparison.Equals -> value == threshold
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        total.add(weight)
        if (matches(value)) matched.add(weight)
    }

    override fun merge(values: RatioResult) {
        matched.add(values.matched)
        total.add(values.total)
    }

    override fun reset() {
        matched.store(0.0)
        total.store(0.0)
    }

    override fun read(timestampNanos: Long) = RatioResult(
        threshold = threshold,
        comparison = comparison,
        matched = matched.load(),
        total = total.load(),
    )

    override fun create(concurrency: Concurrency?) =
        RatioVsTargetStat(threshold, comparison, concurrency ?: this.concurrency)
}

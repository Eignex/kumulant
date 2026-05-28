package com.eignex.kumulant.stat.anomaly

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs
import kotlin.math.sqrt

/**
 * Snapshot from [GaussianScorerStat]: running mean / variance plus the most
 * recently scored value. The `score(x)` helper computes a z-score `|x - mean| /
 * stdDev` on demand using the captured stats.
 */
@Serializable
@SerialName("GaussianScoreResult")
data class GaussianScoreResult(
    /** Running mean. */
    val mean: Double,
    /** Running variance. */
    val variance: Double,
    /** Cumulative observation weight folded in. */
    val totalWeights: Double,
) : Result {

    /** Sample standard deviation. */
    val stdDev: Double get() = sqrt(variance)

    /**
     * Absolute z-score `|x - mean| / stdDev`. Returns 0.0 when the running
     * variance is degenerate (zero or near-zero) so a constant stream produces
     * no false anomaly signal.
     */
    fun score(x: Double): Double {
        val s = stdDev
        return if (s > Z_SCORE_FLOOR) abs(x - mean) / s else 0.0
    }

    private companion object {
        const val Z_SCORE_FLOOR: Double = 1e-12
    }
}

/**
 * Streaming Gaussian anomaly scorer: tracks running mean and variance and
 * returns the absolute z-score `|x - mean| / stdDev` of the most recent value.
 * High scores flag observations that lie far from the running centre.
 *
 * The internal Welford accumulator is updated **before** the score is captured,
 * so the snapshot's `mean` and `variance` already include the latest sample.
 * This matches the River semantics where `score_one(x)` is computed against
 * the post-update state.
 *
 * **Memory:** O(1); wraps a [VarianceStat].
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Inherits [VarianceStat]'s Welford-coupled cells; honours
 * the [Concurrency] level passed in.
 */
class GaussianScorerStat(override val concurrency: Concurrency = Concurrency.None) :
    SeriesStat<GaussianScoreResult> {

    private val inner = VarianceStat(concurrency)

    override fun update(value: Double, timestampNanos: Long, weight: Double) =
        inner.update(value, timestampNanos, weight)

    override fun read(timestampNanos: Long): GaussianScoreResult {
        val r = inner.read(timestampNanos)
        return GaussianScoreResult(mean = r.mean, variance = r.variance, totalWeights = r.totalWeights)
    }

    override fun merge(values: GaussianScoreResult) =
        inner.merge(WeightedVarianceResult(values.totalWeights, values.mean, values.variance))

    override fun reset() = inner.reset()

    override fun create(concurrency: Concurrency?) = GaussianScorerStat(concurrency ?: this.concurrency)
}

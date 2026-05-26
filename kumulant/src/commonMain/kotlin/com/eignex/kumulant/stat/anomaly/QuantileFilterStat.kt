package com.eignex.kumulant.stat.anomaly

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.quantile.DDSketchStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Snapshot from [QuantileFilterStat]: the running [probability]-quantile of
 * the input stream plus the helper [score] that flags an observation as
 * anomalous when it exceeds that quantile.
 */
@Serializable
@SerialName("QuantileFilterResult")
data class QuantileFilterResult(
    /** Probability at which the threshold is evaluated, in `(0, 1)`. */
    val probability: Double,
    /** Estimated quantile of the input stream at [probability]. */
    val threshold: Double,
    /** Cumulative observation weight folded into the underlying sketch. */
    val totalWeights: Double,
) : Result {

    /** `1.0` when [x] strictly exceeds [threshold], `0.0` otherwise. */
    fun score(x: Double): Double = if (x > threshold) 1.0 else 0.0
}

/**
 * Streaming quantile-threshold anomaly detector. Tracks the input distribution
 * via a [DDSketchStat] and exposes the q-quantile as a threshold; the result's
 * `score(x)` helper flags `x > threshold` as a binary anomaly.
 *
 * The threshold adapts with the stream: as new observations arrive, the
 * quantile drifts, so the same input value may flip between anomalous and
 * normal as the distribution shifts. Use [QuantileFilterStat] when you want a
 * non-parametric alternative to [GaussianScorerStat] (no Gaussianity
 * assumption) or when the metric of interest is "is `x` in the tail of what
 * we've seen?".
 *
 * **Memory:** O(1 / `relativeError`) — backed by a single-probability DDSketch.
 *
 * **Update:** O(1) per observation (one striped bin increment).
 *
 * **Concurrency:** Inherits [DDSketchStat]'s additive-mode striped counters;
 * lock-free under every [Concurrency] level.
 */
class QuantileFilterStat(
    /** Probability in `(0, 1)` at which the threshold is evaluated. */
    val probability: Double = 0.99,
    /** Relative-error guarantee passed to the underlying DDSketch. */
    val relativeError: Double = 0.01,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<QuantileFilterResult> {

    init {
        require(probability > 0.0 && probability < 1.0) {
            "probability must be in (0, 1); got $probability"
        }
    }

    private val inner = DDSketchStat(
        relativeError = relativeError,
        probabilities = doubleArrayOf(probability),
        concurrency = concurrency,
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) =
        inner.update(value, timestampNanos, weight)

    override fun read(timestampNanos: Long): QuantileFilterResult {
        val r = inner.read(timestampNanos)
        val q = if (r.quantiles.isNotEmpty()) r.quantiles[0] else Double.NaN
        return QuantileFilterResult(probability = probability, threshold = q, totalWeights = r.totalWeights)
    }

    /**
     * Merge is unsupported: [QuantileFilterResult] only carries the threshold
     * scalar, not the underlying DDSketch bin layout, so two snapshots cannot
     * be combined into a faithful joint distribution. Anomaly detectors are
     * not typically sharded across streams; if you need a distributed
     * quantile, merge a [DDSketchStat] directly and project here.
     */
    override fun merge(values: QuantileFilterResult): Nothing =
        throw UnsupportedOperationException("QuantileFilterStat does not support merge; merge a DDSketch directly")

    override fun reset() = inner.reset()

    override fun create(concurrency: Concurrency?) =
        QuantileFilterStat(probability, relativeError, concurrency ?: this.concurrency)
}

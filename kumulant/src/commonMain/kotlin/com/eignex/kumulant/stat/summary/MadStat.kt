package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.quantile.TDigestStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs

/** Streaming median and median absolute deviation. */
@Serializable
@SerialName("MadResult")
data class MadResult(
    /** Estimated stream median. */
    val median: Double,
    /** Median absolute deviation from the running median estimate. */
    val mad: Double,
) : Result,
    HasCenterScale {
    override val center: Double get() = median
    override val scale: Double get() = mad
}

/**
 * Streaming median absolute deviation, the robust analog of standard deviation. Backed
 * by two [TDigestStat]s: one over raw values (for the running median estimate), one
 * over `|value - median|` (for the MAD itself). The deviation digest is fed against the
 * running median estimate at each update; early observations therefore see a biased
 * median, so the MAD takes ~tens to ~hundreds of updates to stabilise.
 *
 * **Use cases:** robust z-scores, IQR-style outlier fences, robust band centers when
 * the input is heavy-tailed and standard deviation overstates spread.
 *
 * **Memory:** O(compression); two T-digests at the configured [compression].
 *
 * **Update:** O(1) amortised; two T-digest updates plus one median lookup.
 *
 * **Concurrency:** Inherits the underlying T-digest concurrency mode. The pair is
 * loosely coupled (the deviation digest reads the value digest's median on each
 * update); strict ordering between value and deviation updates is not guaranteed
 * under contention, but neither digest drifts beyond its own per-stat guarantees.
 */
class MadStat(
    /** T-digest compression for both digests; lower = more centroids, tighter quantiles. */
    val compression: Double = 100.0,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<MadResult> {

    init {
        require(compression > 0.0) { "compression must be > 0" }
    }

    private val valueDigest = TDigestStat(
        compression = compression,
        probabilities = doubleArrayOf(0.5),
        concurrency = concurrency,
    )
    private val deviationDigest = TDigestStat(
        compression = compression,
        probabilities = doubleArrayOf(0.5),
        concurrency = concurrency,
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        valueDigest.update(value, timestampNanos, weight)
        val medianEstimate = valueDigest.read(timestampNanos).quantiles[0]
        deviationDigest.update(abs(value - medianEstimate), timestampNanos, weight)
    }

    override fun merge(values: MadResult, workspace: com.eignex.koblas.Workspace?) {
        // No round-trippable inner-digest state on the result, so merge approximates by
        // re-pushing the (median, MAD) pair into the two digests as a single weighted update.
        // Sufficient for "combine snapshots" use cases; exact replay isn't possible without
        // shipping both digests on the wire.
        valueDigest.update(values.median, weight = 1.0)
        deviationDigest.update(values.mad, weight = 1.0)
    }

    override fun reset() {
        valueDigest.reset()
        deviationDigest.reset()
    }

    override fun read(timestampNanos: Long): MadResult {
        val medianValue = valueDigest.read(timestampNanos).quantiles[0]
        val medianDeviation = deviationDigest.read(timestampNanos).quantiles[0]
        return MadResult(median = medianValue, mad = medianDeviation)
    }

    override fun create(concurrency: Concurrency?) =
        MadStat(compression = compression, concurrency = concurrency ?: this.concurrency)
}

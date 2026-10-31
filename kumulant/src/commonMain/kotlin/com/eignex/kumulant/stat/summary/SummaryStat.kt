package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasMinMax
import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Snapshot exposing running mean, variance, min, and max simultaneously. */
@Serializable
@SerialName("SummaryResult")
data class SummaryResult(
    override val totalWeights: Double,
    /** Weighted running mean. */
    val mean: Double,
    override val variance: Double,
    override val min: Double,
    override val max: Double,
) : Result,
    HasSampleVariance,
    HasCenterScale,
    HasMinMax {
    override val center: Double get() = mean
    override val scale: Double get() = stdDev
}

/**
 * Comprehensive summary stat; Welford mean/variance plus monotonic min/max in one
 * accumulator. The result implements both [HasCenterScale] (center=mean, scale=stdDev)
 * and [HasMinMax] (min, max), so feedback projections can address Center/Scale and
 * Low/High on the same primary. Use it when a per-coordinate primary fan-out needs to
 * support both standardisation and min-max scaling.
 *
 * **Use cases:** mixed feature scaling in one feedback pipeline; any consumer that
 * wants both moment-based and extremum-based statistics in a single read.
 *
 * **Memory:** O(1); five doubles plus a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Composes a [VarianceStat] and a [RangeStat]. Under [Concurrency.Strict]
 * and [Concurrency.HighWrite] one lock covers both, so each update is atomic and a read is
 * coherent; under [Concurrency.Relaxed] the lock drops, the variance cells drift as in
 * [VarianceStat] and the extrema stay exact; never throws.
 */
class SummaryStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<SummaryResult> {

    // The shared lock keeps variance and extrema coherent in one read, so the parts only need their own
    // atomic cells where that lock is absent: Relaxed, mirroring welfordMode().
    private val partConcurrency = if (concurrency == Concurrency.Relaxed) Concurrency.Relaxed else Concurrency.None
    private val lock = concurrency.welfordLock()
    private val moments = VarianceStat(partConcurrency)
    private val range = RangeStat(partConcurrency)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // The extrema have no inverse, so they move only after the variance accepts the weight: a
        // downdate it rejects must not leave the stat reporting a value from a refused observation.
        if (weight.isInertWeight()) return
        lock.guarded {
            moments.update(value, timestampNanos, weight)
            range.update(value, timestampNanos, weight)
        }
    }

    override fun merge(values: SummaryResult) {
        // read() sanitises the empty sentinels to 0.0, so folding in an untouched shard would drag
        // min down (or, on an all-negative stream, max up) to a value never observed.
        if (values.totalWeights <= 0.0) return
        lock.guarded {
            moments.merge(WeightedVarianceResult(values.totalWeights, values.mean, values.variance))
            range.merge(RangeResult(values.min, values.max))
        }
    }

    override fun reset() = lock.guarded {
        moments.reset()
        range.reset()
    }

    override fun read(timestampNanos: Long): SummaryResult = lock.guarded {
        val m = moments.read(timestampNanos)
        val r = range.read(timestampNanos)
        SummaryResult(
            totalWeights = m.totalWeights,
            mean = m.mean,
            variance = m.variance,
            min = if (r.min == Double.POSITIVE_INFINITY) 0.0 else r.min,
            max = if (r.max == Double.NEGATIVE_INFINITY) 0.0 else r.max,
        )
    }

    override fun create(concurrency: Concurrency?): SummaryStat = SummaryStat(concurrency ?: this.concurrency)
}

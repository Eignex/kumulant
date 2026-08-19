package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasMinMax
import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireLiveWeight
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.casMin
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Snapshot exposing running mean, variance, min, and max simultaneously. */
@Serializable
@SerialName("SummaryResult")
data class SummaryResult(
    /** Cumulative observation weight folded in. */
    override val totalWeights: Double,
    /** Weighted running mean. */
    val mean: Double,
    /** Population variance: `sst / totalWeights`. */
    override val variance: Double,
    /** Running minimum value. */
    override val min: Double,
    /** Running maximum value. */
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
 * **Concurrency:** Welford-coupled mean/variance cells (same model as [VarianceStat])
 * with independent monotonic min/max via [casMin]/[casMax]. Under [Concurrency.Strict]
 * and [Concurrency.HighWrite] the body is locked so each update is atomic; under
 * [Concurrency.Relaxed] the lock drops and the cells race with bounded drift; never
 * throws.
 */
class SummaryStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<SummaryResult> {

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val sst = mode.newDouble(0.0)

    private val monotonic = concurrency.monotonicMode()
    private val minCell = monotonic.newDouble(Double.POSITIVE_INFINITY)
    private val maxCell = monotonic.newDouble(Double.NEGATIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // The extrema have no inverse, so nothing may move min or max before the weight is accepted:
        // neither an inert weight nor a downdate requireLiveWeight goes on to reject, or the stat is
        // left reporting a value from an observation it refused.
        if (weight.isInertWeight()) return
        lock.guarded {
            val priorW = totalWeights.load()
            requireLiveWeight(priorW, weight)
            casMin(minCell, value)
            casMax(maxCell, value)
            val nextW = totalWeights.addAndGet(weight)
            val delta = value - mean.load()
            val r = delta * (weight / nextW)
            mean.add(r)
            sst.add(priorW * delta * r)
        }
    }

    override fun merge(values: SummaryResult) {
        // read() sanitises the empty sentinels to 0.0, so folding in an untouched shard would drag
        // min down (or, on an all-negative stream, max up) to a value never observed.
        if (values.totalWeights <= 0.0) return
        casMin(minCell, values.min)
        casMax(maxCell, values.max)
        lock.guarded {
            val w1 = totalWeights.load()
            val w2 = values.totalWeights
            val nextW = totalWeights.addAndGet(w2)
            val delta = values.mean - mean.load()
            mean.add(delta * (w2 / nextW))
            sst.add(values.variance * w2 + (delta * delta) * (w1 * w2 / nextW))
        }
    }

    override fun reset() = lock.guarded {
            totalWeights.store(0.0)
            mean.store(0.0)
            sst.store(0.0)
            minCell.store(Double.POSITIVE_INFINITY)
            maxCell.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long): SummaryResult = lock.guarded {
        val w = totalWeights.load()
        val variance = if (w > 0.0) sst.load() / w else 0.0
        val lo = minCell.load().let { if (it == Double.POSITIVE_INFINITY) 0.0 else it }
        val hi = maxCell.load().let { if (it == Double.NEGATIVE_INFINITY) 0.0 else it }
        SummaryResult(totalWeights = w, mean = mean.load(), variance = variance, min = lo, max = hi)
    }

    override fun create(concurrency: Concurrency?): SummaryStat = SummaryStat(concurrency ?: this.concurrency)
}

package com.eignex.kumulant.stat.change

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.max
import kotlin.math.min

/** Snapshot from a [PageHinkleyStat] change-point detector. */
@Serializable
@SerialName("PageHinkleyResult")
data class PageHinkleyResult(
    /** Tolerance applied on each side; positive deviations smaller than this are absorbed. */
    val delta: Double,
    /** Alarm threshold; the alarm fires when either drift exceeds this. */
    val threshold: Double,
    /** Number of observations folded in. */
    val count: Long,
    /** Running mean of all observations. */
    val mean: Double,
    /** Cumulative positive-drift signal `m_t = Sum (x_i - mean - delta)`. */
    val cumulativePositive: Double,
    /** Cumulative negative-drift signal `m_t = Sum (x_i - mean + delta)`. */
    val cumulativeNegative: Double,
    /** Running minimum of [cumulativePositive], used as the baseline for the upward test. */
    val minPositive: Double,
    /** Running maximum of [cumulativeNegative], used as the baseline for the downward test. */
    val maxNegative: Double,
    /** True when `cumulativePositive - minPositive > threshold`. */
    val alarmUp: Boolean,
    /** True when `maxNegative - cumulativeNegative > threshold`. */
    val alarmDown: Boolean,
) : Result {
    /** True when either drift test has fired. */
    val alarm: Boolean get() = alarmUp || alarmDown
}

/**
 * Page-Hinkley change-point detector. Tracks the running mean alongside two
 * one-sided cumulative-drift signals
 *
 * ```
 * m+_t = Sum (x_i - mean - delta),  M+_t = min m+_t
 * m-_t = Sum (x_i - mean + delta),  M-_t = max m-_t
 * ```
 *
 * and raises an alarm when `m+_t - M+_t > threshold` (upward drift) or
 * `M-_t - m-_t > threshold` (downward drift). The tolerance [delta] absorbs
 * in-control fluctuation; [threshold] controls false-alarm rate.
 *
 * **Use cases:** drift detection in monitored signals, model-residual whiteness
 * loss alarms, online change-point detection where CUSUM's known target value
 * is not available.
 *
 * **Memory:** O(1) — six cells plus a lock.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Coupled mean / positive / negative / extrema cells (category 3).
 * The internal lock keeps the multi-cell update consistent.
 */
class PageHinkleyStat(
    /** Tolerance absorbing in-control fluctuation; must be `>= 0`. */
    val delta: Double = 0.005,
    /** Alarm threshold for either drift; must be `>= 0`. */
    val threshold: Double = 50.0,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<PageHinkleyResult> {

    init {
        require(delta >= 0.0) { "delta must be >= 0, got $delta" }
        require(threshold >= 0.0) { "threshold must be >= 0, got $threshold" }
    }

    private val streamMode = concurrency.monotonicMode()
    private val lock = concurrency.serializedLock()
    private val count = streamMode.newLong(0L)
    private val mean = streamMode.newDouble(0.0)
    private val cumPos = streamMode.newDouble(0.0)
    private val cumNeg = streamMode.newDouble(0.0)
    private val minPos = streamMode.newDouble(0.0)
    private val maxNeg = streamMode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        val n = count.load() + 1L
        count.store(n)
        val prevMean = mean.load()
        val nextMean = prevMean + (value - prevMean) / n.toDouble()
        mean.store(nextMean)
        val deviation = value - nextMean
        val cp = cumPos.load() + deviation - delta
        val cn = cumNeg.load() + deviation + delta
        cumPos.store(cp)
        cumNeg.store(cn)
        minPos.store(min(minPos.load(), cp))
        maxNeg.store(max(maxNeg.load(), cn))
    }

    override fun merge(values: PageHinkleyResult) = lock.withLock {
        // Approximate merge: weighted-average mean, then average the cumulative-drift cells.
        val localCount = count.load()
        val incomingCount = values.count
        val combinedCount = localCount + incomingCount
        if (combinedCount == 0L) return@withLock
        val combinedMean =
            (mean.load() * localCount + values.mean * incomingCount) / combinedCount.toDouble()
        count.store(combinedCount)
        mean.store(combinedMean)
        cumPos.store(0.5 * (cumPos.load() + values.cumulativePositive))
        cumNeg.store(0.5 * (cumNeg.load() + values.cumulativeNegative))
        minPos.store(min(minPos.load(), values.minPositive))
        maxNeg.store(max(maxNeg.load(), values.maxNegative))
    }

    override fun reset() = lock.withLock {
        count.store(0L)
        mean.store(0.0)
        cumPos.store(0.0)
        cumNeg.store(0.0)
        minPos.store(0.0)
        maxNeg.store(0.0)
    }

    override fun read(timestampNanos: Long) = lock.withLock {
        val cp = cumPos.load()
        val cn = cumNeg.load()
        val mp = minPos.load()
        val mn = maxNeg.load()
        PageHinkleyResult(
            delta = delta,
            threshold = threshold,
            count = count.load(),
            mean = mean.load(),
            cumulativePositive = cp,
            cumulativeNegative = cn,
            minPositive = mp,
            maxNegative = mn,
            alarmUp = (cp - mp) > threshold,
            alarmDown = (mn - cn) > threshold,
        )
    }

    override fun create(concurrency: Concurrency?) = PageHinkleyStat(delta, threshold, concurrency ?: this.concurrency)
}

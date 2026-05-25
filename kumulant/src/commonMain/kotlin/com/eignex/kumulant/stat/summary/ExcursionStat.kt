package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running peak / trough excursion summary. */
@Serializable
@SerialName("ExcursionResult")
data class ExcursionResult(
    /** Highest value observed so far. */
    val peak: Double,
    /** Timestamp (nanoseconds) at which [peak] was set. */
    val peakTimestampNanos: Long,
    /** Lowest value observed strictly after the [peak] was last set. */
    val trough: Double,
    /** Timestamp of [trough]; equal to [peakTimestampNanos] if no post-peak observation exists. */
    val troughTimestampNanos: Long,
    /** Largest `peak_at_time - subsequent_trough` observed across the stream. */
    val maxExcursion: Double,
    /** `peak - lastValue`; how far the latest observation sits below the all-time peak. */
    val currentRecovery: Double,
) : Result

/**
 * Tracks the running peak of the input stream, the lowest value seen since the
 * peak was last set, and the largest peak-to-subsequent-trough excursion
 * observed across history.
 *
 * When a new peak arrives, the post-peak trough resets to that peak.
 * [ExcursionResult.maxExcursion] is monotonic: it only grows.
 *
 * **Use cases:** running-high water tracking with drawdown / recovery
 * monitoring, generic excursion analysis.
 *
 * **Memory:** O(1) — six cells plus a lock.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Internal CAS spin-lock (category 3). Coupled peak / trough
 * state cannot be made elementwise atomic; the lock keeps the snapshot
 * consistent under any [Concurrency] level.
 */
class ExcursionStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<ExcursionResult> {

    private val mode = concurrency.monotonicMode()
    private val lock = concurrency.serializedLock()
    private val initialized = mode.newLong(0L)
    private val peak = mode.newDouble(Double.NEGATIVE_INFINITY)
    private val peakTs = mode.newLong(0L)
    private val trough = mode.newDouble(Double.POSITIVE_INFINITY)
    private val troughTs = mode.newLong(0L)
    private val maxExcursion = mode.newDouble(0.0)
    private val lastValue = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        lastValue.store(value)
        val seen = initialized.addAndGet(1L)
        if (seen == 1L) {
            peak.store(value)
            peakTs.store(timestampNanos)
            trough.store(value)
            troughTs.store(timestampNanos)
            return@withLock
        }
        if (value > peak.load()) {
            peak.store(value)
            peakTs.store(timestampNanos)
            trough.store(value)
            troughTs.store(timestampNanos)
        } else if (value < trough.load()) {
            trough.store(value)
            troughTs.store(timestampNanos)
            val excursion = peak.load() - value
            if (excursion > maxExcursion.load()) maxExcursion.store(excursion)
        }
    }

    override fun merge(values: ExcursionResult) = lock.withLock {
        val seen = initialized.addAndGet(1L)
        if (seen == 1L) {
            peak.store(values.peak)
            peakTs.store(values.peakTimestampNanos)
            trough.store(values.trough)
            troughTs.store(values.troughTimestampNanos)
            maxExcursion.store(values.maxExcursion)
            lastValue.store(values.peak - values.currentRecovery)
            return@withLock
        }
        if (values.peak > peak.load()) {
            peak.store(values.peak)
            peakTs.store(values.peakTimestampNanos)
        }
        if (values.trough < trough.load()) {
            trough.store(values.trough)
            troughTs.store(values.troughTimestampNanos)
        }
        if (values.maxExcursion > maxExcursion.load()) maxExcursion.store(values.maxExcursion)
    }

    override fun reset() = lock.withLock {
        initialized.store(0L)
        peak.store(Double.NEGATIVE_INFINITY)
        peakTs.store(0L)
        trough.store(Double.POSITIVE_INFINITY)
        troughTs.store(0L)
        maxExcursion.store(0.0)
        lastValue.store(0.0)
    }

    override fun read(timestampNanos: Long) = lock.withLock {
        if (initialized.load() == 0L) {
            ExcursionResult(0.0, 0L, 0.0, 0L, 0.0, 0.0)
        } else {
            ExcursionResult(
                peak = peak.load(),
                peakTimestampNanos = peakTs.load(),
                trough = trough.load(),
                troughTimestampNanos = troughTs.load(),
                maxExcursion = maxExcursion.load(),
                currentRecovery = peak.load() - lastValue.load(),
            )
        }
    }

    override fun create(concurrency: Concurrency?) = ExcursionStat(concurrency ?: this.concurrency)
}

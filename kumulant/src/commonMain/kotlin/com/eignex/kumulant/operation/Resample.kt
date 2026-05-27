package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stream.additiveMode
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.Serializable
import kotlin.math.max
import kotlin.math.min
import kotlin.time.Duration

// resampleByTime aligns the input stream onto fixed wall-clock buckets and forwards
// one observation per closed bucket to the delegate stat. The per-bucket value is
// determined by [ResampleAggregator]; the in-progress bucket is held until an update
// arrives in a later bucket (or a flush is requested).
//
// Concurrency: coupled per-bucket state and bucket index (category 3). The internal
// lock keeps the multi-cell transition consistent.

/** Per-bucket reduction used by [resampleByTime]. */
@Serializable
enum class ResampleAggregator {
    /** Forward the arithmetic mean of in-bucket values (unweighted). */
    Mean,

    /** Forward the sum of in-bucket values. */
    Sum,

    /** Forward the most recent in-bucket value. */
    Last,

    /** Forward the minimum in-bucket value. */
    Min,

    /** Forward the maximum in-bucket value. */
    Max,
}

/** Align this series on fixed wall-clock buckets of [bucket] length and forward one
 *  observation per closed bucket using [aggregator]. */
internal fun <R : Result> SeriesStat<R>.resampleByTime(
    bucket: Duration,
    aggregator: ResampleAggregator = ResampleAggregator.Mean,
): SeriesStat<R> = ResampleByTimeStat(this, bucket, aggregator)

internal class ResampleByTimeStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val bucket: Duration,
    private val aggregator: ResampleAggregator,
) : SeriesStat<R>,
    Stat<R> by delegate {

    private val bucketNanos: Long = bucket.inWholeNanoseconds

    init {
        require(bucketNanos > 0L) { "resampleByTime requires a positive bucket duration" }
    }

    private val streamMode = delegate.concurrency.additiveMode()
    private val lock = delegate.concurrency.serializedLock()
    private val currentBucket = streamMode.newLong(NO_BUCKET)
    private val count = streamMode.newLong(0L)
    private val sum = streamMode.newDouble(0.0)
    private val last = streamMode.newDouble(0.0)
    private val minimum = streamMode.newDouble(Double.POSITIVE_INFINITY)
    private val maximum = streamMode.newDouble(Double.NEGATIVE_INFINITY)
    private val bucketEndTimestamp = streamMode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        val newBucket = timestampNanos.floorDiv(bucketNanos)
        val cur = currentBucket.load()
        if (cur == NO_BUCKET) {
            currentBucket.store(newBucket)
            seed(value, timestampNanos)
            return@withLock
        }
        if (newBucket == cur) {
            accumulate(value, timestampNanos)
            return@withLock
        }
        // Bucket changed: flush the closed bucket and seed the new one.
        flushLocked()
        currentBucket.store(newBucket)
        seed(value, timestampNanos)
    }

    private fun seed(value: Double, timestampNanos: Long) {
        count.store(1L)
        sum.store(value)
        last.store(value)
        minimum.store(value)
        maximum.store(value)
        bucketEndTimestamp.store(timestampNanos)
    }

    private fun accumulate(value: Double, timestampNanos: Long) {
        count.store(count.load() + 1L)
        sum.store(sum.load() + value)
        last.store(value)
        minimum.store(min(minimum.load(), value))
        maximum.store(max(maximum.load(), value))
        bucketEndTimestamp.store(timestampNanos)
    }

    private fun flushLocked() {
        val n = count.load()
        if (n <= 0L) return
        val value = when (aggregator) {
            ResampleAggregator.Mean -> sum.load() / n.toDouble()
            ResampleAggregator.Sum -> sum.load()
            ResampleAggregator.Last -> last.load()
            ResampleAggregator.Min -> minimum.load()
            ResampleAggregator.Max -> maximum.load()
        }
        delegate.update(value, bucketEndTimestamp.load(), weight = 1.0)
    }

    override fun reset() = lock.withLock {
        delegate.reset()
        currentBucket.store(NO_BUCKET)
        count.store(0L)
        sum.store(0.0)
        last.store(0.0)
        minimum.store(Double.POSITIVE_INFINITY)
        maximum.store(Double.NEGATIVE_INFINITY)
        bucketEndTimestamp.store(0L)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        ResampleByTimeStat(delegate.create(concurrency), bucket, aggregator)

    companion object {
        private const val NO_BUCKET: Long = Long.MIN_VALUE
    }
}

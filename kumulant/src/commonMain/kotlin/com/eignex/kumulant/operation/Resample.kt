package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireLiveWeight
import com.eignex.kumulant.schema.spec.ResampleAggregator
import com.eignex.kumulant.stream.additiveMode
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock
import kotlin.math.max
import kotlin.math.min
import kotlin.time.Duration

// resampleByTime aligns the input stream onto fixed wall-clock buckets and forwards
// one observation per closed bucket to the delegate stat. The per-bucket value is
// determined by [ResampleAggregator].
//
// An update in a later bucket is the only thing that closes the open one. There is no
// flush, and `read` does not close it either, so the newest bucket is never visible: a
// reader always sees the stream as of the last bucket boundary an update crossed. That
// is what makes the operator's output independent of when it is read, which a windowed
// or periodically-scraped consumer depends on - a flush on read would emit a partial
// bucket whose value then changed as the rest of it arrived.
//
// Concurrency: coupled per-bucket state and bucket index (category 3). The internal
// lock keeps the multi-cell transition consistent.

/**
 * Align this series on fixed wall-clock buckets of [bucket] length and forward one
 * observation per closed bucket using [aggregator].
 *
 * A bucket closes when an update arrives in a later one, so the delegate lags the input by up to one
 * bucket and the open bucket is never readable. Reads are therefore stable: the same read repeated
 * returns the same value until an update crosses a boundary.
 *
 * Caller weight folds into the open bucket rather than passing through: [ResampleAggregator.Sum] and
 * [ResampleAggregator.Mean] weight the observations they combine, and the closed bucket then reaches
 * the delegate as a single observation of weight `1.0` however many raw ones went into it.
 */
internal fun <R : Result> SeriesStat<R>.resampleByTime(
    bucket: Duration,
    aggregator: ResampleAggregator = ResampleAggregator.Mean,
): SeriesStat<R> = ResampleByTimeStat(this, bucket, aggregator)

internal class ResampleByTimeStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val bucket: Duration,
    private val aggregator: ResampleAggregator,
) : SeriesStat<R>,
    WindowsInside<R>,
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

    // Total caller weight folded into the open bucket, so Sum and Mean can respect it. The flush
    // itself always carries weight 1.0: a closed bucket is one derived observation regardless of how
    // many raw ones went into it.
    private val bucketWeight = streamMode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            val newBucket = timestampNanos.floorDiv(bucketNanos)
            val cur = currentBucket.load()
            // The same guard the Welford stats downstream apply, for the same reason: Mean divides by
            // the bucket's accumulated weight, so a downdate that takes it to zero publishes an
            // infinity that every later read inherits. A downdate can only remove what the open bucket
            // already holds, and a bucket not yet open holds nothing, hence the zero prior.
            //
            // Checked before any cell is written, so a rejected update leaves the stat as it was
            // rather than half-applied.
            requireLiveWeight(if (cur == newBucket) bucketWeight.load() else 0.0, weight)
            if (cur == NO_BUCKET) {
                currentBucket.store(newBucket)
                seed(value, timestampNanos, weight)
                return@guarded
            }
            if (newBucket <= cur) {
                // A late arrival joins the open bucket rather than closing it: its own bucket was
                // already emitted, and closing on any change would emit the open one twice.
                accumulate(value, timestampNanos, weight)
                return@guarded
            }
            // Time moved on: flush the closed bucket and seed the new one.
            flushLocked()
            currentBucket.store(newBucket)
            seed(value, timestampNanos, weight)
        }
    }

    private fun seed(value: Double, timestampNanos: Long, weight: Double) {
        bucketWeight.store(weight)
        count.store(1L)
        sum.store(value * weight)
        last.store(value)
        minimum.store(value)
        maximum.store(value)
        bucketEndTimestamp.store(timestampNanos)
    }

    private fun accumulate(value: Double, timestampNanos: Long, weight: Double) {
        bucketWeight.store(bucketWeight.load() + weight)
        count.store(count.load() + 1L)
        sum.store(sum.load() + value * weight)
        minimum.store(min(minimum.load(), value))
        maximum.store(max(maximum.load(), value))
        // Guarded against a late arrival: it must not become the bucket's "last" value, nor drag the
        // bucket's end timestamp backwards.
        if (timestampNanos >= bucketEndTimestamp.load()) {
            last.store(value)
            bucketEndTimestamp.store(timestampNanos)
        }
    }

    private fun flushLocked() {
        val n = count.load()
        if (n <= 0L) return
        val value = when (aggregator) {
            ResampleAggregator.Mean -> sum.load() / bucketWeight.load()
            ResampleAggregator.Sum -> sum.load()
            ResampleAggregator.Last -> last.load()
            ResampleAggregator.Min -> minimum.load()
            ResampleAggregator.Max -> maximum.load()
        }
        delegate.update(value, bucketEndTimestamp.load(), weight = 1.0)
    }

    override fun reset() = lock.guarded {
        delegate.reset()
        currentBucket.store(NO_BUCKET)
        count.store(0L)
        sum.store(0.0)
        last.store(0.0)
        minimum.store(Double.POSITIVE_INFINITY)
        maximum.store(Double.NEGATIVE_INFINITY)
        bucketEndTimestamp.store(0L)
        bucketWeight.store(0.0)
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        ResampleByTimeStat(delegate.windowed(duration, slices, concurrency), bucket, aggregator)

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        ResampleByTimeStat(delegate.create(concurrency), bucket, aggregator)

    companion object {
        private const val NO_BUCKET: Long = Long.MIN_VALUE
    }
}

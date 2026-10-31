package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireLiveWeight
import com.eignex.kumulant.schema.spec.ResampleAggregator
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock
import kotlin.math.max
import kotlin.math.min
import kotlin.time.Duration

// Only updates crossing a bucket boundary publish a bucket, making reads independent of polling time.
// The bucket index and accumulated value share a lock for consistent transitions.

// Updates crossing a bucket boundary close the prior bucket. Closed buckets carry weight 1;
// Sum and Mean incorporate caller weights inside the bucket.
internal fun <R : Result> SeriesStat<R>.resampleByTime(
    bucket: Duration,
    aggregator: ResampleAggregator = ResampleAggregator.Mean,
): SeriesStat<R> = ResampleByTimeStat(this, bucket, aggregator)

internal class ResampleByTimeStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val bucket: Duration,
    private val aggregator: ResampleAggregator,
) : SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {

    private val bucketNanos: Long = bucket.inWholeNanoseconds

    init {
        require(bucketNanos > 0L) { "resampleByTime requires a positive bucket duration" }
    }

    // Every cell is touched only under the lock and most are not additive, so striped adders would
    // only turn the min/max stores into relative adds that go NaN at an infinite seed.
    private val streamMode = delegate.concurrency.monotonicMode()
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
            // Validate the destination bucket weight before mutation to keep rejected downdates atomic.
            // Late arrivals contribute to the open bucket; seed paths start with zero weight.
            val joinsOpenBucket = cur != NO_BUCKET && newBucket <= cur
            requireLiveWeight(if (joinsOpenBucket) bucketWeight.load() else 0.0, weight)
            if (cur == NO_BUCKET) {
                currentBucket.store(newBucket)
                seed(value, timestampNanos, weight)
                return@guarded
            }
            if (joinsOpenBucket) {
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

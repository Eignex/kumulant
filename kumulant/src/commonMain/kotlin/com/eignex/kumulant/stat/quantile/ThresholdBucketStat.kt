package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Per-bucket counts for a user-defined threshold list. For thresholds `[t1, t2, ..., tK]`
 * (strictly increasing) the result holds `K + 1` counts; bucket `i` contains
 * `t[i-1] < value <= t[i]` with the open-ended ends `value <= t[0]` and `value > t[K-1]`.
 */
@Serializable
@SerialName("ThresholdBucketResult")
data class ThresholdBucketResult(
    /** Strictly increasing thresholds used to define the buckets. */
    val thresholds: List<Double>,
    /** Per-bucket weighted counts; length is `thresholds.size + 1`. */
    val counts: List<Double>,
) : Result

/**
 * Weighted counter over user-defined value buckets.
 *
 * **Use cases:** distribution-shape monitoring with caller-chosen bin edges,
 * complementary to the auto-binned quantile sketches.
 *
 * **Memory:** O(K) for K thresholds; one double cell per bucket.
 *
 * **Update:** O(log K); binary search over the threshold list, single atomic
 * add into the resolved bucket.
 *
 * **Concurrency:** Per-bucket atomic adds (category 1). Exact under every
 * [Concurrency] level since each update touches exactly one cell.
 */
class ThresholdBucketStat(
    /** Strictly increasing threshold edges. */
    val thresholds: DoubleArray,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<ThresholdBucketResult> {

    init {
        require(thresholds.isNotEmpty()) { "thresholds must be non-empty" }
        for (i in 1 until thresholds.size) {
            require(thresholds[i] > thresholds[i - 1]) {
                "thresholds must be strictly increasing, found ${thresholds[i - 1]} >= ${thresholds[i]} at index $i"
            }
        }
    }

    private val mode = concurrency.additiveMode()
    private val counts = mode.newDoubleArray(thresholds.size + 1)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // bucketFor compares against each threshold, and every comparison against NaN is false, so a
        // NaN fell through to the overflow bucket and was counted as the largest possible value.
        if (weight.isInertWeight()) return
        counts.add(bucketFor(value), weight)
    }

    private fun bucketFor(value: Double): Int {
        var lo = 0
        var hi = thresholds.size
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (value <= thresholds[mid]) hi = mid else lo = mid + 1
        }
        return lo
    }

    override fun merge(values: ThresholdBucketResult) {
        require(values.counts.size == thresholds.size + 1) {
            "merge counts length ${values.counts.size} != ${thresholds.size + 1}"
        }
        values.counts.forEachIndexed { i, c -> counts.add(i, c) }
    }

    override fun reset() {
        for (i in 0..thresholds.size) counts.store(i, 0.0)
    }

    override fun read(timestampNanos: Long) = ThresholdBucketResult(
        thresholds = thresholds.toList(),
        counts = List(thresholds.size + 1) { counts.load(it) },
    )

    override fun create(concurrency: Concurrency?) =
        ThresholdBucketStat(thresholds.copyOf(), concurrency ?: this.concurrency)
}

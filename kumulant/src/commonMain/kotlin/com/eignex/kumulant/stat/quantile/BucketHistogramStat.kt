package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.additiveMode
import kotlin.math.nextUp

/**
 * Weighted histogram over fixed, caller-chosen bucket edges.
 *
 * For edges `[e0, e1, ..., eK]` (strictly increasing) the stat keeps `K + 2` buckets:
 * `(-inf, e0]`, `(e(i-1), e(i)]` for each consecutive pair, and `(eK, +inf)`. Each bucket
 * includes its upper edge, the `le` convention of Prometheus-style latency buckets.
 * [linear] builds evenly spaced edges.
 *
 * Counts are additive, so a negative weight downdates its bucket and a count can go
 * negative; callers must not read one as a population.
 *
 * Merge adds an interval of the incoming snapshot to the bucket holding its midpoint (or its
 * finite end, for an open interval). A snapshot over the same edges merges exactly; one from
 * another layout, such as [SketchResult.toSparseHistogram], is rebinned approximately.
 *
 * **Use cases:** distribution-shape monitoring with caller-chosen bin edges, SLO latency
 * buckets, calibration and probability histograms over a known range. Reach for
 * [DDSketchStat] for unbounded values with logarithmic resolution.
 *
 * **Memory:** O(K); one double cell per bucket.
 *
 * **Update:** O(log K); binary search over the edges, single atomic add into the bucket.
 *
 * **Concurrency:** Per-bucket atomic adds. Exact under every [Concurrency] level since each
 * update touches exactly one cell.
 */
class BucketHistogramStat(
    /** Strictly increasing, finite bucket edges. */
    edges: DoubleArray,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<SparseHistogramResult> {

    // Copied so a caller mutating its array cannot move the edges under recorded counts.
    private val edges: DoubleArray = edges.copyOf()

    init {
        require(this.edges.isNotEmpty()) { "edges must be non-empty" }
        require(this.edges.all { it.isFinite() }) { "edges must be finite" }
        for (i in 1 until this.edges.size) {
            require(this.edges[i] > this.edges[i - 1]) {
                "edges must be strictly increasing, found ${this.edges[i - 1]} >= ${this.edges[i]} at index $i"
            }
        }
    }

    /** Bucket edges, in increasing order. */
    val bucketEdges: List<Double> get() = edges.toList()

    private val counts = concurrency.additiveMode().newDoubleArray(edges.size + 1)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        counts.add(bucketFor(value), weight)
    }

    private fun bucketFor(value: Double): Int {
        var lo = 0
        var hi = edges.size
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (value <= edges[mid]) hi = mid else lo = mid + 1
        }
        return lo
    }

    override fun merge(values: SparseHistogramResult, timestampNanos: Long) {
        for (i in values.weights.indices) {
            val w = values.weights[i]
            if (w.isInertWeight()) continue
            val lo = values.lowerBounds[i]
            val hi = values.upperBounds[i]
            val representative = when {
                lo.isFinite() && hi.isFinite() -> lo + (hi - lo) / 2.0
                hi.isFinite() -> hi
                lo.isFinite() -> lo.nextUp()
                else -> continue
            }
            counts.add(bucketFor(representative), w)
        }
    }

    override fun reset() {
        for (i in 0..edges.size) counts.store(i, 0.0)
    }

    override fun read(timestampNanos: Long): SparseHistogramResult {
        val snapshot = DoubleArray(edges.size + 1) { counts.load(it) }
        val populated = snapshot.indices.filter { snapshot[it] != 0.0 }
        return SparseHistogramResult(
            lowerBounds = DoubleArray(populated.size) { lowerEdge(populated[it]) },
            upperBounds = DoubleArray(populated.size) { upperEdge(populated[it]) },
            weights = DoubleArray(populated.size) { snapshot[populated[it]] },
        )
    }

    private fun lowerEdge(bucket: Int): Double = if (bucket == 0) Double.NEGATIVE_INFINITY else edges[bucket - 1]

    private fun upperEdge(bucket: Int): Double = if (bucket == edges.size) Double.POSITIVE_INFINITY else edges[bucket]

    override fun create(concurrency: Concurrency?) = BucketHistogramStat(edges, concurrency ?: this.concurrency)

    /** Factories for common edge layouts. */
    companion object {
        /** Histogram with [binCount] equal-width buckets over `(lowerBound, upperBound]` plus the two open ends. */
        fun linear(
            lowerBound: Double,
            upperBound: Double,
            binCount: Int,
            concurrency: Concurrency = Concurrency.None,
        ): BucketHistogramStat = BucketHistogramStat(linearEdges(lowerBound, upperBound, binCount), concurrency)
    }
}

/** [binCount] + 1 evenly spaced edges from [lowerBound] to [upperBound], both inclusive. */
fun linearEdges(lowerBound: Double, upperBound: Double, binCount: Int): DoubleArray {
    require(lowerBound.isFinite() && upperBound.isFinite()) { "bounds must be finite" }
    require(upperBound > lowerBound) { "upperBound must be greater than lowerBound" }
    require(binCount > 0) { "binCount must be > 0" }
    val width = (upperBound - lowerBound) / binCount
    // The last edge is pinned to upperBound so accumulated rounding cannot exclude it.
    return DoubleArray(binCount + 1) { if (it == binCount) upperBound else lowerBound + it * width }
}

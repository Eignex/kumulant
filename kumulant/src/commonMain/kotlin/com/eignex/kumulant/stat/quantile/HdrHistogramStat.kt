package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.additiveMode
import kotlin.math.ceil
import kotlin.math.log2
import kotlin.math.pow
import kotlin.math.round

/**
 * Auto-resizing High Dynamic Range (HDR) Histogram with native Double support.
 *
 * By defining a [lowestDiscernibleValue], the histogram internally scales
 * floating-point metrics into integers for O(1) bitwise routing, preserving
 * fractional precision down to that resolution.
 *
 * **Use cases:** latency and SLO percentile reporting where bin-based
 * quantiles with absolute error bounds are required. Reach for this over
 * [DDSketchStat] when the value range is bounded and [lowestDiscernibleValue]
 * is meaningful; over [TDigestStat] when bucket counts (rather than centroids)
 * are wanted in the snapshot.
 *
 * **Memory:** O([significantDigits] · log2(highest/lowest)) buckets; grows
 * to accommodate values past [initialHighestTrackableValue].
 *
 * **Update:** O(1) per observation; bitwise bucket assignment + striped
 * atomic add.
 *
 * **Concurrency:** Striped atomic adds on independent buckets. Lock-free and
 * exact under every [Concurrency] level; bucket increments commute and
 * assignment is deterministic per value.
 */
class HdrHistogramStat(
    /** Smallest value the histogram can distinguish. */
    val lowestDiscernibleValue: Double = 0.001,
    /** Initial upper bound; the histogram grows past this if needed. */
    val initialHighestTrackableValue: Double = 100.0,
    /** Number of significant digits of precision (1..5). */
    val significantDigits: Int = 3,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<SparseHistogramResult> {

    private val mode = concurrency.additiveMode()

    init {
        require(lowestDiscernibleValue > 0.0) { "Lowest discernible value must be > 0" }
        require(initialHighestTrackableValue > lowestDiscernibleValue * 2) {
            "Highest trackable value must be at least 2x the lowest discernible value"
        }
        require(significantDigits in 1..5) { "Significant digits must be between 1 and 5" }
    }

    // The scale factor converts inbound Doubles to internal Longs, and outbound Longs to Doubles
    private val multiplier: Double = 1.0 / lowestDiscernibleValue

    // Math constants for bucket density
    private val subBucketHalfCountMagnitude =
        ceil(log2(10.0.pow(significantDigits))).toInt()
    private val subBucketHalfCount = 1 shl subBucketHalfCountMagnitude
    private val subBucketCount = subBucketHalfCount shl 1

    private class State(
        val highestTrackableValue: Long, // Stored as scaled internal Long
        val counts: Array<StreamDouble>,
    )

    private val stateRef = mode.newReference(
        createState(
            (initialHighestTrackableValue * multiplier).toLong(),
            emptyArray(),
        ),
    )

    private fun createState(internalHighest: Long, oldCounts: Array<StreamDouble>): State {
        // Ensure the internal highest is at least 2 to prevent bitwise math collapse
        val safeHighest = if (internalHighest < 2L) 2L else internalHighest

        val highestBit = 63 - safeHighest.countLeadingZeroBits()
        val maxBucketIndex = highestBit - subBucketHalfCountMagnitude

        val newCountsArrayLength = if (maxBucketIndex <= 0) {
            subBucketCount
        } else {
            subBucketCount + (maxBucketIndex * subBucketHalfCount)
        }

        val newCounts = Array(newCountsArrayLength) { i ->
            if (i < oldCounts.size) oldCounts[i] else mode.newDouble(0.0)
        }

        return State(safeHighest, newCounts)
    }

    private fun tryResize(oldState: State, newInternalValue: Long) {
        var newHighest = oldState.highestTrackableValue

        while (newHighest < newInternalValue && newHighest > 0) {
            newHighest = newHighest shl 1
        }

        if (newHighest <= 0) newHighest = Long.MAX_VALUE

        val newState = createState(newHighest, oldState.counts)
        stateRef.compareAndSet(oldState, newState)
    }

    private fun getIndex(internalValue: Long): Int {
        if (internalValue < subBucketCount) return internalValue.toInt()

        val highestBit = 63 - internalValue.countLeadingZeroBits()
        val bucketIndex = highestBit - subBucketHalfCountMagnitude
        val subBucketIndex =
            (internalValue ushr bucketIndex).toInt() and (subBucketHalfCount - 1)

        return subBucketCount + (bucketIndex - 1) * subBucketHalfCount + subBucketIndex
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return
        // `!(value < 0.0)`, not `value >= 0.0`: both reject a negative value, but every comparison
        // against NaN is false, so the `>=` form would reject NaN as though it were negative and
        // throw. A NaN observation must not become an exception in the caller. See Stat.
        require(!(value < 0.0)) { "HdrHistogramStat only supports non-negative values; got $value" }

        addInternal((value * multiplier).toLong(), weight)
    }

    private fun addInternal(internalValue: Long, weight: Double) {
        while (true) {
            val state = stateRef.load()

            if (internalValue > state.highestTrackableValue) {
                tryResize(state, internalValue)
                continue
            }

            state.counts[getIndex(internalValue)].add(weight)
            return
        }
    }

    override fun create(concurrency: Concurrency?) = HdrHistogramStat(
        lowestDiscernibleValue,
        initialHighestTrackableValue,
        significantDigits,
        concurrency ?: this.concurrency,
    )

    override fun merge(values: SparseHistogramResult, workspace: com.eignex.koblas.Workspace?) {
        for (i in values.lowerBounds.indices) {
            val weight = values.weights[i]
            if (weight > 0.0) {
                val bound = values.lowerBounds[i]
                // The same non-negative, finite domain update enforces. SparseHistogramResult is the
                // shared merge type for every histogram and sketch projection, so a bound from a
                // DDSketch that saw negative values, or the -Infinity floor of a LinearHistogram
                // underflow row, arrives here through ordinary public API. Unguarded, getIndex hands
                // back the negative value unchanged and the counts array is indexed out of bounds -
                // and -Infinity is worse than that, since its Long is Int-truncated to 0 and the weight
                // is filed silently into the lowest bucket.
                require(bound.isFinite() && bound >= 0.0) {
                    "merge bucket lower bound must be finite and non-negative, got $bound"
                }
                // Rounded, not truncated: a bound this histogram emitted is an exact bucket floor
                // divided by the multiplier, and multiplying it back lands a hair under the integer.
                addInternal(round(bound * multiplier).toLong(), weight)
            }
        }
    }

    override fun reset() {
        while (true) {
            val state = stateRef.load()
            val fresh = createState(
                (initialHighestTrackableValue * multiplier).toLong(),
                emptyArray(),
            )
            if (stateRef.compareAndSet(state, fresh)) return
        }
    }

    private fun getLowerBound(index: Int): Long {
        if (index < subBucketCount) return index.toLong()

        val bucketIndex = ((index - subBucketCount) / subBucketHalfCount) + 1
        val subBucketIndex = (index - subBucketCount) % subBucketHalfCount

        return ((1L shl subBucketHalfCountMagnitude) + subBucketIndex) shl bucketIndex
    }

    private fun getUpperBound(index: Int): Long {
        if (index < subBucketCount) return index.toLong() + 1L

        val bucketIndex = ((index - subBucketCount) / subBucketHalfCount) + 1
        return getLowerBound(index) + (1L shl bucketIndex)
    }

    override fun read(timestampNanos: Long): SparseHistogramResult {
        val state = stateRef.load()

        // Snapshot every cell once, then size and fill from that snapshot. Loading the cells
        // twice would let a bucket become populated between the counting and filling passes, so
        // the fill overruns the arrays sized by the first pass.
        val snapshot = DoubleArray(state.counts.size) { state.counts[it].load() }

        var populatedCount = 0
        for (w in snapshot) {
            if (w > 0.0) populatedCount++
        }

        val lowers = DoubleArray(populatedCount)
        val uppers = DoubleArray(populatedCount)
        val weights = DoubleArray(populatedCount)

        var cursor = 0
        for (i in snapshot.indices) {
            val w = snapshot[i]
            if (w > 0.0) {
                lowers[cursor] = getLowerBound(i).toDouble() / multiplier
                uppers[cursor] = getUpperBound(i).toDouble() / multiplier
                weights[cursor] = w
                cursor++
            }
        }

        return SparseHistogramResult(lowers, uppers, weights)
    }
}

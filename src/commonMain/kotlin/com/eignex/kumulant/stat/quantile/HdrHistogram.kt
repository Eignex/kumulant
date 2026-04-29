package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlin.math.ceil
import kotlin.math.log2
import kotlin.math.pow

/**
 * A lock-free, auto-resizing High Dynamic Range (HDR) Histogram with native Double support.
 * By defining a lowestDiscernibleValue, it internally scales floating-point metrics
 * into integers for O(1) bitwise routing, perfectly preserving fractional precision.
 */
class HdrHistogram(
    val lowestDiscernibleValue: Double = 0.001,
    val initialHighestTrackableValue: Double = 100.0,
    val significantDigits: Int = 3,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<SparseHistogramResult> {

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
        val counts: Array<StreamDouble>
    )

    private val stateRef = mode.newReference(
        createState(
            (initialHighestTrackableValue * multiplier).toLong(),
            emptyArray()
        )
    )
    private val _totalWeights = mode.newDouble(0.0)

    private fun createState(
        internalHighest: Long,
        oldCounts: Array<StreamDouble>
    ): State {
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
        if (weight <= 0.0 || value < 0.0) return // HDR only supports >= 0

        // Scale the incoming floating-point value to an internal integer
        val internalValue = (value * multiplier).toLong()

        _totalWeights.add(weight)

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

    override fun create(mode: StreamMode?) = HdrHistogram(
        lowestDiscernibleValue,
        initialHighestTrackableValue,
        significantDigits,
        mode ?: this.mode
    )

    override fun merge(values: SparseHistogramResult) {
        for (i in values.lowerBounds.indices) {
            val weight = values.weights[i]
            if (weight > 0.0) {
                update(values.lowerBounds[i], weight)
            }
        }
    }

    override fun reset() {
        _totalWeights.store(0.0)
        val state = stateRef.load()
        for (i in state.counts.indices) {
            state.counts[i].store(0.0)
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

        var populatedCount = 0
        for (i in state.counts.indices) {
            if (state.counts[i].load() > 0.0) populatedCount++
        }

        val lowers = DoubleArray(populatedCount)
        val uppers = DoubleArray(populatedCount)
        val weights = DoubleArray(populatedCount)

        var cursor = 0
        for (i in state.counts.indices) {
            val w = state.counts[i].load()
            if (w > 0.0) {
                // Divide the internal integer boundaries back down to the original Double scale
                lowers[cursor] = getLowerBound(i).toDouble() / multiplier
                uppers[cursor] = getUpperBound(i).toDouble() / multiplier
                weights[cursor] = w
                cursor++
            }
        }

        return SparseHistogramResult(lowers, uppers, weights)
    }
}

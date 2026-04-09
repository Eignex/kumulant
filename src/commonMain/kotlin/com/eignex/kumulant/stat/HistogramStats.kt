package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.*
import com.eignex.kumulant.concurrent.ArrayBins
import com.eignex.kumulant.concurrent.StreamDouble
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.HasQuantile
import com.eignex.kumulant.core.QuantileResult
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.SketchResult
import com.eignex.kumulant.core.SparseHistogramResult
import kotlin.math.*

class FrugalQuantile(
    val q: Double,
    val stepSize: Double = 0.01,
    val initialEstimate: Double = 0.0,
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<QuantileResult>, HasQuantile {

    init {
        require(q in 0.0..1.0) { "Quantile q must be between 0.0 and 1.0" }
    }

    private val _estimate = mode.newDouble(initialEstimate)

    override val probability: Double get() = q
    override val quantile: Double by _estimate

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return

        val m = _estimate.load()
        val delta = if (value > m) {
            stepSize * q * weight
        } else if (value < m) {
            -stepSize * (1.0 - q) * weight
        } else {
            0.0
        }

        if (delta != 0.0) {
            _estimate.add(delta)
        }
    }

    override fun create(
        mode: StreamMode?,
        name: String?
    ) = FrugalQuantile(
        q,
        stepSize,
        initialEstimate,
        mode ?: this.mode,
        name ?: this.name
    )

    override fun merge(values: QuantileResult) {
        val current = _estimate.load()
        _estimate.store((current + values.quantile) / 2.0)
    }

    override fun reset() {
        _estimate.store(initialEstimate)
    }

    override fun read(timestampNanos: Long) = QuantileResult(q, quantile, name)
}

class DDSketch(
    val relativeError: Double = 0.01,
    val probabilities: DoubleArray = doubleArrayOf(
        0.5,
        0.75,
        0.9,
        0.95,
        0.99,
        0.999
    ),
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<SketchResult> {

    init {
        require(relativeError in 0.0..1.0) { "Relative error must be between 0.0 and 1.0" }
    }

    private val gamma: Double = (1.0 + relativeError) / (1.0 - relativeError)
    private val multiplier: Double = 1.0 / ln(gamma)

    private val _totalWeights = mode.newDouble(0.0)
    private val _zeroCount = mode.newDouble(0.0)

    private val positiveBins = ArrayBins(mode)
    private val negativeBins = ArrayBins(mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        _totalWeights.add(weight)

        if (value > 0.0) {
            val index = ceil(ln(value) * multiplier).toInt()
            positiveBins.add(index, weight)
        } else if (value < 0.0) {
            val index = ceil(ln(-value) * multiplier).toInt()
            negativeBins.add(index, weight)
        } else {
            _zeroCount.add(weight)
        }
    }

    override fun create(
        mode: StreamMode?,
        name: String?
    ) = DDSketch(
        relativeError,
        probabilities,
        mode ?: this.mode,
        name ?: this.name
    )

    override fun merge(values: SketchResult) {
        require(abs(this.gamma - values.gamma) < 1e-9) {
            "Cannot merge DDSketches with different relative error targets"
        }

        _totalWeights.add(values.totalWeights)
        _zeroCount.add(values.zeroCount)

        values.positiveBins.forEach { (index, weight) ->
            if (weight > 0.0) positiveBins.add(index, weight)
        }
        values.negativeBins.forEach { (index, weight) ->
            if (weight > 0.0) negativeBins.add(index, weight)
        }
    }

    override fun reset() {
        _totalWeights.store(0.0)
        _zeroCount.store(0.0)
        positiveBins.clear()
        negativeBins.clear()
    }

    private fun valueFromIndex(index: Int): Double {
        return 2.0 * gamma.pow(index) / (1.0 + gamma)
    }

    override fun read(timestampNanos: Long): SketchResult {
        val total = _totalWeights.load()
        val computedQuantiles = DoubleArray(probabilities.size)

        val posSnap = positiveBins.snapshot()
        val negSnap = negativeBins.snapshot()
        val zeroSnap = _zeroCount.load()

        if (total == 0.0) {
            return SketchResult(
                probabilities = probabilities,
                quantiles = computedQuantiles,
                gamma = gamma,
                totalWeights = total,
                zeroCount = zeroSnap,
                positiveBins = posSnap,
                negativeBins = negSnap,
                name = name
            )
        }

        // Sort bins: negative bins descending (most negative to 0), positive ascending (0 to max)
        val sortedNeg = negSnap.entries.sortedByDescending { it.key }
        val sortedPos = posSnap.entries.sortedBy { it.key }

        fun computeQuantile(targetRank: Double): Double {
            var currentRank = 0.0
            for ((index, weight) in sortedNeg) {
                currentRank += weight
                if (currentRank >= targetRank) return -valueFromIndex(index)
            }
            currentRank += zeroSnap
            if (currentRank >= targetRank) return 0.0
            for ((index, weight) in sortedPos) {
                currentRank += weight
                if (currentRank >= targetRank) return valueFromIndex(index)
            }
            return Double.NaN
        }

        for (i in probabilities.indices) {
            computedQuantiles[i] = computeQuantile(probabilities[i] * total)
        }

        return SketchResult(
            probabilities = probabilities,
            quantiles = computedQuantiles,
            gamma = gamma,
            totalWeights = total,
            zeroCount = zeroSnap,
            positiveBins = posSnap,
            negativeBins = negSnap,
            name = name
        )
    }
}

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
    override val name: String? = null
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

    override fun create(
        mode: StreamMode?,
        name: String?
    ) = HdrHistogram(
        lowestDiscernibleValue,
        initialHighestTrackableValue,
        significantDigits,
        mode ?: this.mode,
        name ?: this.name
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

        return SparseHistogramResult(lowers, uppers, weights, name)
    }
}

package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.ArrayBins
import com.eignex.kumulant.concurrent.StreamDouble
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.QuantileResult
import com.eignex.kumulant.core.ReservoirResult
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.SketchResult
import com.eignex.kumulant.core.SparseHistogramResult
import com.eignex.kumulant.core.TDigestResult
import kotlin.math.*
import kotlin.random.Random

/**
 * Frugal-streaming single-quantile estimator.
 *
 * Keeps one `Double` of state that drifts toward the target quantile [q]; the drift
 * magnitude is scaled by [stepSize]. Cheap and memory-flat but biased and noisy —
 * use [DDSketch] when accuracy matters. Not intended for merging beyond averaging.
 */
class FrugalQuantile(
    val q: Double,
    val stepSize: Double = 0.01,
    val initialEstimate: Double = 0.0,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<QuantileResult> {

    init {
        require(q in 0.0..1.0) { "Quantile q must be between 0.0 and 1.0" }
    }

    private val quantile = mode.newDouble(initialEstimate)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return

        val m = quantile.load()
        val delta = if (value > m) {
            stepSize * q * weight
        } else if (value < m) {
            -stepSize * (1.0 - q) * weight
        } else {
            0.0
        }

        if (delta != 0.0) {
            quantile.add(delta)
        }
    }

    override fun create(mode: StreamMode?) = FrugalQuantile(
        q,
        stepSize,
        initialEstimate,
        mode ?: this.mode
    )

    override fun merge(values: QuantileResult) {
        val current = quantile.load()
        quantile.store((current + values.quantile) / 2.0)
    }

    override fun reset() {
        quantile.store(initialEstimate)
    }

    override fun read(timestampNanos: Long) = QuantileResult(q, quantile.load())
}

/**
 * DDSketch: relative-error quantile sketch with logarithmic bins.
 *
 * Guarantees [relativeError] on every reported quantile using `O(log(max/min))`
 * bins. Supports negative values via a mirrored bin tree and a zero-bucket.
 * Tightening [relativeError] grows bin count roughly as `1/ε`.
 */
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

    override fun create(mode: StreamMode?) = DDSketch(
        relativeError,
        probabilities,
        mode ?: this.mode
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
                negativeBins = negSnap
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
            negativeBins = negSnap
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

/**
 * Fixed-width binned histogram over `[lowerBound, upperBound)` split into [binCount] buckets.
 *
 * Values below or at/above the range fall into dedicated underflow / overflow rows
 * `(NEG_INFINITY, lowerBound)` and `[upperBound, POS_INFINITY)`. Bin storage is
 * lock-free via [ArrayBins].
 */
class LinearHistogram(
    val lowerBound: Double,
    val upperBound: Double,
    val binCount: Int,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<SparseHistogramResult> {

    init {
        require(lowerBound.isFinite() && upperBound.isFinite()) {
            "Bounds must be finite"
        }
        require(upperBound > lowerBound) {
            "upperBound must be greater than lowerBound"
        }
        require(binCount > 0) { "binCount must be > 0" }
    }

    private val binWidth: Double = (upperBound - lowerBound) / binCount

    private val _totalWeights = mode.newDouble(0.0)
    private val _underflow = mode.newDouble(0.0)
    private val _overflow = mode.newDouble(0.0)
    private val bins = ArrayBins(mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0 || value.isNaN()) return
        _totalWeights.add(weight)

        when {
            value < lowerBound -> _underflow.add(weight)
            value >= upperBound -> _overflow.add(weight)
            else -> {
                val idx = ((value - lowerBound) / binWidth).toInt().coerceIn(0, binCount - 1)
                bins.add(idx, weight)
            }
        }
    }

    override fun create(mode: StreamMode?) = LinearHistogram(
        lowerBound,
        upperBound,
        binCount,
        mode ?: this.mode
    )

    override fun merge(values: SparseHistogramResult) {
        for (i in values.lowerBounds.indices) {
            val w = values.weights[i]
            if (w <= 0.0) continue
            val lo = values.lowerBounds[i]
            val hi = values.upperBounds[i]
            val target = when {
                !lo.isFinite() && hi.isFinite() -> hi - binWidth / 2.0
                lo.isFinite() && !hi.isFinite() -> lo + binWidth / 2.0
                else -> (lo + hi) / 2.0
            }
            update(target, w)
        }
    }

    override fun reset() {
        _totalWeights.store(0.0)
        _underflow.store(0.0)
        _overflow.store(0.0)
        bins.clear()
    }

    override fun read(timestampNanos: Long): SparseHistogramResult {
        val snap = bins.snapshot()
        val under = _underflow.load()
        val over = _overflow.load()

        val populated = snap.size + (if (under > 0.0) 1 else 0) + (if (over > 0.0) 1 else 0)
        val lowers = DoubleArray(populated)
        val uppers = DoubleArray(populated)
        val weights = DoubleArray(populated)

        var cursor = 0
        if (under > 0.0) {
            lowers[cursor] = Double.NEGATIVE_INFINITY
            uppers[cursor] = lowerBound
            weights[cursor] = under
            cursor++
        }
        val sortedKeys = IntArray(snap.size).also {
            var i = 0
            for (k in snap.keys) it[i++] = k
            it.sort()
        }
        for (idx in sortedKeys) {
            lowers[cursor] = lowerBound + idx * binWidth
            uppers[cursor] = lowerBound + (idx + 1) * binWidth
            weights[cursor] = snap.getValue(idx)
            cursor++
        }
        if (over > 0.0) {
            lowers[cursor] = upperBound
            uppers[cursor] = Double.POSITIVE_INFINITY
            weights[cursor] = over
            cursor++
        }
        return SparseHistogramResult(lowers, uppers, weights)
    }
}

/**
 * Weighted reservoir sample of size [capacity] via Algorithm A-Res
 * (Efraimidis & Spirakis): each item gets a key `u^(1/w)` and the top-`k`
 * keys are retained, giving an unbiased weight-proportional sample.
 *
 * State is held behind a CAS-swapped reference; under [com.eignex.kumulant.concurrent.AtomicMode]
 * concurrent updates are racy but eventually consistent. For strict
 * thread-safety, wrap in `.locked()`.
 */
class ReservoirHistogram(
    val capacity: Int = 1024,
    val seed: Long = 0L,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<ReservoirResult> {

    init {
        require(capacity > 0) { "capacity must be > 0" }
    }

    private class State(
        val values: DoubleArray,
        val keys: DoubleArray,
        val totalSeen: Long,
        val totalWeight: Double
    )

    private val random = Random(seed)
    private val stateRef = mode.newReference(
        State(DoubleArray(0), DoubleArray(0), 0L, 0.0)
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0 || value.isNaN()) return

        val u = random.nextDouble()
        val key = if (weight == 1.0) u else u.pow(1.0 / weight)

        while (true) {
            val s = stateRef.load()
            val newState: State
            if (s.values.size < capacity) {
                val newVals = s.values.copyOf(s.values.size + 1)
                val newKeys = s.keys.copyOf(s.keys.size + 1)
                newVals[s.values.size] = value
                newKeys[s.keys.size] = key
                newState = State(newVals, newKeys, s.totalSeen + 1, s.totalWeight + weight)
            } else {
                var minIdx = 0
                var minKey = s.keys[0]
                for (i in 1 until s.keys.size) {
                    if (s.keys[i] < minKey) {
                        minKey = s.keys[i]
                        minIdx = i
                    }
                }
                if (key > minKey) {
                    val newVals = s.values.copyOf()
                    val newKeys = s.keys.copyOf()
                    newVals[minIdx] = value
                    newKeys[minIdx] = key
                    newState = State(newVals, newKeys, s.totalSeen + 1, s.totalWeight + weight)
                } else {
                    newState = State(s.values, s.keys, s.totalSeen + 1, s.totalWeight + weight)
                }
            }
            if (stateRef.compareAndSet(s, newState)) return
        }
    }

    override fun create(mode: StreamMode?) = ReservoirHistogram(
        capacity,
        seed,
        mode ?: this.mode
    )

    override fun merge(values: ReservoirResult) {
        require(values.values.size == values.keys.size) {
            "ReservoirResult values/keys size mismatch"
        }
        while (true) {
            val s = stateRef.load()
            val combinedVals = s.values + values.values
            val combinedKeys = s.keys + values.keys

            val (vOut, kOut) = if (combinedVals.size <= capacity) {
                combinedVals to combinedKeys
            } else {
                val indices = (combinedKeys.indices).sortedByDescending { combinedKeys[it] }
                    .take(capacity)
                val v = DoubleArray(capacity) { combinedVals[indices[it]] }
                val k = DoubleArray(capacity) { combinedKeys[indices[it]] }
                v to k
            }
            val newState = State(
                vOut,
                kOut,
                s.totalSeen + values.totalSeen,
                s.totalWeight + values.totalWeight
            )
            if (stateRef.compareAndSet(s, newState)) return
        }
    }

    override fun reset() {
        stateRef.store(State(DoubleArray(0), DoubleArray(0), 0L, 0.0))
    }

    override fun read(timestampNanos: Long): ReservoirResult {
        val s = stateRef.load()
        return ReservoirResult(
            values = s.values.copyOf(),
            keys = s.keys.copyOf(),
            capacity = capacity,
            totalSeen = s.totalSeen,
            totalWeight = s.totalWeight
        )
    }
}

/**
 * Buffered merging T-Digest (Dunning) with `k1` scaling function for high-fidelity
 * extreme-quantile estimates and bounded centroid count. [compression] (δ) caps
 * centroids to roughly `~6·δ`.
 *
 * Updates buffer values until [bufferCap] is reached, then fold them into the
 * sorted centroid list under the `k1`-difference ≤ 1 merge rule. State is
 * CAS-swapped immutables so SerialMode is cheap and AtomicMode is correct
 * (though not lock-free under heavy contention).
 */
class TDigest(
    val compression: Double = 100.0,
    val probabilities: DoubleArray = doubleArrayOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<TDigestResult> {

    init {
        require(compression > 0.0) { "compression must be > 0" }
        for (p in probabilities) {
            require(p in 0.0..1.0) { "probabilities must be in [0,1]" }
        }
    }

    private val bufferCap: Int = max(10, (5.0 * compression).toInt())

    private class State(
        val means: DoubleArray,
        val weights: DoubleArray,
        val totalWeight: Double,
        val buffer: DoubleArray,
        val bufferWeights: DoubleArray,
        val bufferLen: Int
    )

    private fun emptyState() = State(
        DoubleArray(0),
        DoubleArray(0),
        0.0,
        DoubleArray(bufferCap),
        DoubleArray(bufferCap),
        0
    )

    private val stateRef = mode.newReference(emptyState())

    private fun k1(q: Double): Double =
        compression / (2.0 * PI) * asin(2.0 * q.coerceIn(0.0, 1.0) - 1.0)

    private fun compress(s: State): State {
        if (s.bufferLen == 0 && s.means.isEmpty()) return s

        val n = s.means.size + s.bufferLen
        val combinedM = DoubleArray(n)
        val combinedW = DoubleArray(n)

        var i = 0
        var j = 0
        var c = 0
        val bufIdx = (0 until s.bufferLen).sortedBy { s.buffer[it] }
        while (i < s.means.size && j < s.bufferLen) {
            val bv = s.buffer[bufIdx[j]]
            if (s.means[i] <= bv) {
                combinedM[c] = s.means[i]
                combinedW[c] = s.weights[i]
                i++
            } else {
                combinedM[c] = bv
                combinedW[c] = s.bufferWeights[bufIdx[j]]
                j++
            }
            c++
        }
        while (i < s.means.size) {
            combinedM[c] = s.means[i]
            combinedW[c] = s.weights[i]
            i++; c++
        }
        while (j < s.bufferLen) {
            combinedM[c] = s.buffer[bufIdx[j]]
            combinedW[c] = s.bufferWeights[bufIdx[j]]
            j++; c++
        }

        val totalWeight = s.totalWeight
        if (totalWeight <= 0.0) return emptyState()

        val outM = DoubleArray(n)
        val outW = DoubleArray(n)
        var outLen = 0

        var curM = combinedM[0]
        var curW = combinedW[0]
        var qLeft = 0.0
        var kLeft = k1(qLeft)

        for (idx in 1 until n) {
            val nextM = combinedM[idx]
            val nextW = combinedW[idx]
            val combinedQRight = qLeft + (curW + nextW) / totalWeight
            val kRight = k1(combinedQRight)
            if (kRight - kLeft <= 1.0) {
                val mergedW = curW + nextW
                curM = curM + (nextM - curM) * nextW / mergedW
                curW = mergedW
            } else {
                outM[outLen] = curM
                outW[outLen] = curW
                outLen++
                qLeft += curW / totalWeight
                kLeft = k1(qLeft)
                curM = nextM
                curW = nextW
            }
        }
        outM[outLen] = curM
        outW[outLen] = curW
        outLen++

        return State(
            outM.copyOf(outLen),
            outW.copyOf(outLen),
            totalWeight,
            DoubleArray(bufferCap),
            DoubleArray(bufferCap),
            0
        )
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0 || value.isNaN()) return

        while (true) {
            val s = stateRef.load()
            val newBuf = s.buffer.copyOf()
            val newBufW = s.bufferWeights.copyOf()
            newBuf[s.bufferLen] = value
            newBufW[s.bufferLen] = weight
            val staged = State(
                s.means,
                s.weights,
                s.totalWeight + weight,
                newBuf,
                newBufW,
                s.bufferLen + 1
            )
            val finalState = if (staged.bufferLen >= bufferCap) compress(staged) else staged
            if (stateRef.compareAndSet(s, finalState)) return
        }
    }

    override fun create(mode: StreamMode?) = TDigest(
        compression,
        probabilities,
        mode ?: this.mode
    )

    override fun merge(values: TDigestResult) {
        require(abs(compression - values.compression) < 1e-9) {
            "Cannot merge TDigests with different compression"
        }
        for (i in values.means.indices) {
            update(values.means[i], 0L, values.weights[i])
        }
    }

    override fun reset() {
        stateRef.store(emptyState())
    }

    override fun read(timestampNanos: Long): TDigestResult {
        while (true) {
            val s = stateRef.load()
            if (s.bufferLen == 0) break
            val compressed = compress(s)
            if (stateRef.compareAndSet(s, compressed)) break
        }

        val s = stateRef.load()
        val means = s.means
        val weights = s.weights
        val total = s.totalWeight

        val computed = DoubleArray(probabilities.size)
        if (means.isEmpty() || total <= 0.0) {
            return TDigestResult(probabilities, computed, means.copyOf(), weights.copyOf(), total, compression)
        }

        // Cumulative rank at each centroid's center (half-weight offsets).
        val centers = DoubleArray(means.size)
        var acc = 0.0
        for (i in means.indices) {
            centers[i] = acc + weights[i] / 2.0
            acc += weights[i]
        }

        for (pi in probabilities.indices) {
            val targetRank = probabilities[pi] * total
            val n = means.size
            val q: Double
            if (n == 1 || targetRank <= centers[0]) {
                q = means[0]
            } else if (targetRank >= centers[n - 1]) {
                q = means[n - 1]
            } else {
                var idx = 0
                for (i in 0 until n - 1) {
                    if (targetRank <= centers[i + 1]) {
                        idx = i; break
                    }
                }
                val span = centers[idx + 1] - centers[idx]
                val frac = if (span <= 0.0) 0.0 else (targetRank - centers[idx]) / span
                q = means[idx] + frac * (means[idx + 1] - means[idx])
            }
            computed[pi] = q
        }

        return TDigestResult(
            probabilities = probabilities,
            quantiles = computed,
            means = means.copyOf(),
            weights = weights.copyOf(),
            totalWeight = total,
            compression = compression
        )
    }
}

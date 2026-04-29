package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.ArrayBins
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.SparseHistogramResult

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

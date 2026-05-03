package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.ArrayBins
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode

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
    override val mode: StreamMode = defaultStreamMode,
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
            when {
                !lo.isFinite() && hi == lowerBound -> {
                    _totalWeights.add(w)
                    _underflow.add(w)
                }
                lo == upperBound && !hi.isFinite() -> {
                    _totalWeights.add(w)
                    _overflow.add(w)
                }
                lo.isFinite() && hi.isFinite() && matchesLayout(lo, hi) -> {
                    _totalWeights.add(w)
                    val idx = ((lo - lowerBound) / binWidth).toInt().coerceIn(0, binCount - 1)
                    bins.add(idx, w)
                }
                else -> {
                    val target = when {
                        !lo.isFinite() && hi.isFinite() -> hi - binWidth / 2.0
                        lo.isFinite() && !hi.isFinite() -> lo + binWidth / 2.0
                        else -> (lo + hi) / 2.0
                    }
                    update(target, w)
                }
            }
        }
    }

    private fun matchesLayout(lo: Double, hi: Double): Boolean {
        val span = hi - lo
        if (kotlin.math.abs(span - binWidth) > binWidth * 1e-12) return false
        val ratio = (lo - lowerBound) / binWidth
        val rounded = kotlin.math.round(ratio)
        return kotlin.math.abs(ratio - rounded) < 1e-9 && rounded >= 0.0 && rounded < binCount
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

package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stream.ArrayBins
import com.eignex.kumulant.stream.additiveMode
import kotlin.math.abs
import kotlin.math.round

/**
 * Fixed-width binned histogram over `[lowerBound, upperBound)` split into
 * [binCount] buckets.
 *
 * Values below or at/above the range fall into dedicated underflow / overflow
 * rows `(NEG_INFINITY, lowerBound)` and `[upperBound, POS_INFINITY)`.
 *
 * **Use cases:** monitoring distributions over a known range with equal-width
 * buckets; calibration plots, probability histograms, anything with a flat
 * resolution requirement. Reach for [HdrHistogramStat] for unbounded values
 * with logarithmic resolution.
 *
 * **Memory:** O([binCount]) Longs plus two overflow cells.
 *
 * **Update:** O(1) per observation; arithmetic bin assignment + striped
 * atomic add.
 *
 * **Concurrency:** Striped atomic adds on independent bins. Lock-free and
 * exact under every [Concurrency] level; increments commute and bin
 * assignment is deterministic per value.
 */
class LinearHistogramStat(
    /** Inclusive lower bound of the histogram's covered range. */
    val lowerBound: Double,
    /** Exclusive upper bound of the histogram's covered range. */
    val upperBound: Double,
    /** Number of equal-width bins between the bounds. */
    val binCount: Int,
    override val concurrency: Concurrency = Concurrency.None,
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

    private val mode = concurrency.additiveMode()
    private val underflow = mode.newDouble(0.0)
    private val overflow = mode.newDouble(0.0)
    private val bins = ArrayBins(mode)

    // Only the update path: merge resolves its bucket from the emitted edge instead, because
    // `lowerBound + i * binWidth` divided back by binWidth routinely lands just under `i`.
    private fun binOf(value: Double): Int = ((value - lowerBound) / binWidth).toInt().coerceIn(0, binCount - 1)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return

        when {
            value < lowerBound -> underflow.add(weight)

            value >= upperBound -> overflow.add(weight)

            else -> {
                bins.add(binOf(value), weight)
            }
        }
    }

    override fun create(concurrency: Concurrency?) = LinearHistogramStat(
        lowerBound,
        upperBound,
        binCount,
        concurrency ?: this.concurrency,
    )

    override fun merge(values: SparseHistogramResult, workspace: com.eignex.koblas.Workspace?) {
        for (i in values.lowerBounds.indices) {
            val w = values.weights[i]
            if (w <= 0.0) continue
            val lo = values.lowerBounds[i]
            val hi = values.upperBounds[i]
            val aligned = if (lo.isFinite() && hi.isFinite()) alignedBin(lo, hi) else NO_BIN
            when {
                !lo.isFinite() && hi == lowerBound -> {
                    underflow.add(w)
                }

                lo == upperBound && !hi.isFinite() -> {
                    overflow.add(w)
                }

                aligned >= 0 -> {
                    bins.add(aligned, w)
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

    /** Index this bucket was emitted from, or [NO_BIN] when its edges do not match our layout. */
    private fun alignedBin(lo: Double, hi: Double): Int {
        val span = hi - lo
        if (abs(span - binWidth) > binWidth * 1e-12) return NO_BIN
        val ratio = (lo - lowerBound) / binWidth
        val rounded = round(ratio)
        if (abs(ratio - rounded) >= BIN_ALIGNMENT_TOLERANCE || rounded < 0.0 || rounded >= binCount) return NO_BIN
        return rounded.toInt()
    }

    override fun reset() {
        underflow.store(0.0)
        overflow.store(0.0)
        bins.clear()
    }

    override fun read(timestampNanos: Long): SparseHistogramResult {
        val snap = bins.snapshot()
        val under = underflow.load()
        val over = overflow.load()

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

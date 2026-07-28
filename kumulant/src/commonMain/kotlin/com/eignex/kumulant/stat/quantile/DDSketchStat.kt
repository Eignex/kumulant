package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.ArrayBins
import com.eignex.kumulant.stream.additiveMode
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow

/**
 * DDSketchStat: relative-error quantile sketch with logarithmic bins.
 *
 * Guarantees [relativeError] on every reported quantile using `O(log(max/min))`
 * bins. Supports negative values via a mirrored bin tree and a zero-bucket.
 * Tightening [relativeError] grows bin count roughly as `1/epsilon`.
 *
 * **Use cases:** quantile / percentile monitoring of any positive-skewed
 * scalar (latencies, payload sizes). Reach for this over [HdrHistogramStat]
 * when the value range is unbounded; over [TDigestStat] when relative-error
 * guarantees are required.
 *
 * **Memory:** O(log(max/min) / log(1+[relativeError])) bins; typically a few
 * hundred to a few thousand bins for sub-percent error on real-world ranges.
 *
 * **Update:** O(1) per observation; one `log`/bin-assignment + striped atomic add.
 *
 * **Concurrency:** Striped atomic adds on independent bins. Lock-free and
 * exact under every [Concurrency] level; increments commute, bin assignment
 * is deterministic per value.
 */
class DDSketchStat(
    /** Relative error guarantee on every reported quantile. */
    val relativeError: Double = 0.01,
    /** Quantiles to evaluate at read time. */
    val probabilities: DoubleArray = doubleArrayOf(
        0.5,
        0.75,
        0.9,
        0.95,
        0.99,
        0.999,
    ),
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<SketchResult> {

    init {
        require(relativeError > 0.0 && relativeError < 1.0) {
            "Relative error must be strictly between 0.0 and 1.0; got $relativeError"
        }
    }

    private val gamma: Double = (1.0 + relativeError) / (1.0 - relativeError)
    private val multiplier: Double = 1.0 / ln(gamma)

    private val mode = concurrency.additiveMode()
    private val zeroCount = mode.newDouble(0.0)

    private val positiveBins = ArrayBins(mode)
    private val negativeBins = ArrayBins(mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return

        if (value > 0.0) {
            val index = ceil(ln(value) * multiplier).toInt()
            positiveBins.add(index, weight)
        } else if (value < 0.0) {
            val index = ceil(ln(-value) * multiplier).toInt()
            negativeBins.add(index, weight)
        } else {
            zeroCount.add(weight)
        }
    }

    override fun create(concurrency: Concurrency?) = DDSketchStat(
        relativeError,
        probabilities,
        concurrency ?: this.concurrency,
    )

    override fun merge(values: SketchResult) {
        require(abs(this.gamma - values.gamma) < 1e-9) {
            "Cannot merge DDSketches with different relative error targets"
        }

        zeroCount.add(values.zeroCount)

        values.positiveBins.forEach { (index, weight) ->
            if (weight > 0.0) positiveBins.add(index, weight)
        }
        values.negativeBins.forEach { (index, weight) ->
            if (weight > 0.0) negativeBins.add(index, weight)
        }
    }

    override fun reset() {
        zeroCount.store(0.0)
        positiveBins.clear()
        negativeBins.clear()
    }

    private fun valueFromIndex(index: Int): Double = 2.0 * gamma.pow(index) / (1.0 + gamma)

    override fun read(timestampNanos: Long): SketchResult {
        val computedQuantiles = DoubleArray(probabilities.size)

        val posSnap = positiveBins.snapshot()
        val negSnap = negativeBins.snapshot()
        val zeroSnap = zeroCount.load()

        // Derive the total from the snapshot instead of loading the totalWeights cell.
        // update() bumps that cell before it lands the bin, so a concurrent read could
        // see a total larger than the bins account for, walk off the end of the bin list
        // and return NaN for a high quantile. reset() has the mirror problem: it cleared
        // the cell before the bins, so a reader could see total == 0 with populated bins
        // and report all-zero quantiles.
        var total = zeroSnap
        for (w in negSnap.values) total += w
        for (w in posSnap.values) total += w

        if (total <= 0.0) {
            return SketchResult(
                probabilities = probabilities,
                quantiles = computedQuantiles,
                gamma = gamma,
                totalWeights = total,
                zeroCount = zeroSnap,
                positiveBins = posSnap,
                negativeBins = negSnap,
            )
        }

        val sortedNeg = negSnap.entries.sortedByDescending { it.key }
        val sortedPos = posSnap.entries.sortedBy { it.key }

        fun computeQuantile(targetRank: Double): Double {
            var currentRank = 0.0
            for ((index, weight) in sortedNeg) {
                currentRank += weight
                if (currentRank >= targetRank) return -valueFromIndex(index)
            }
            // Only claim the zero bucket when something actually landed in it. Adding an
            // empty zeroSnap and testing `0.0 >= 0.0` made p0 report 0.0 on all-positive
            // data, a value not in the stream and outside the relative-error contract.
            if (zeroSnap > 0.0) {
                currentRank += zeroSnap
                if (currentRank >= targetRank) return 0.0
            }
            for ((index, weight) in sortedPos) {
                currentRank += weight
                if (currentRank >= targetRank) return valueFromIndex(index)
            }
            // The per-bin weights sum to `total` by construction, so the loops above cover
            // every reachable rank. Falling through means floating-point summation left
            // the running rank a hair under the target, which is a rounding artefact
            // rather than a missing bin: report the largest populated bucket.
            sortedPos.lastOrNull()?.let { return valueFromIndex(it.key) }
            if (zeroSnap > 0.0) return 0.0
            sortedNeg.lastOrNull()?.let { return -valueFromIndex(it.key) }
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
        )
    }
}

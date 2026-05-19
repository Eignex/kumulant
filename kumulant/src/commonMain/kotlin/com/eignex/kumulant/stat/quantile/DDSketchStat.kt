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
 * # Concurrency
 *
 * Each bin is a striped atomic add. Lock-free and exact under every
 * [Concurrency] level — increments commute, and the bin assignment is a
 * deterministic function of the value so racing writers on the same value
 * just increment the same cell.
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
    private val totalWeights = mode.newDouble(0.0)
    private val zeroCount = mode.newDouble(0.0)

    private val positiveBins = ArrayBins(mode)
    private val negativeBins = ArrayBins(mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        totalWeights.add(weight)

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
        concurrency ?: this.concurrency
    )

    override fun merge(values: SketchResult) {
        require(abs(this.gamma - values.gamma) < 1e-9) {
            "Cannot merge DDSketches with different relative error targets"
        }

        totalWeights.add(values.totalWeights)
        zeroCount.add(values.zeroCount)

        values.positiveBins.forEach { (index, weight) ->
            if (weight > 0.0) positiveBins.add(index, weight)
        }
        values.negativeBins.forEach { (index, weight) ->
            if (weight > 0.0) negativeBins.add(index, weight)
        }
    }

    override fun reset() {
        totalWeights.store(0.0)
        zeroCount.store(0.0)
        positiveBins.clear()
        negativeBins.clear()
    }

    private fun valueFromIndex(index: Int): Double {
        return 2.0 * gamma.pow(index) / (1.0 + gamma)
    }

    override fun read(timestampNanos: Long): SketchResult {
        val total = totalWeights.load()
        val computedQuantiles = DoubleArray(probabilities.size)

        val posSnap = positiveBins.snapshot()
        val negSnap = negativeBins.snapshot()
        val zeroSnap = zeroCount.load()

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

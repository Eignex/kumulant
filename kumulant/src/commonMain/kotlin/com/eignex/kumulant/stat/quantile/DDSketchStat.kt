package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
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
 * **Memory:** bounded by the indexable range, at
 * `log([maxIndexableValue] / [minIndexableValue]) / log(1 + [relativeError])` bins;
 * roughly 2400 bins at the defaults. Values outside the range fold into the edge
 * bins and still count toward every rank, so the bound holds whatever the input.
 * Narrow the range if you know your data's span and want fewer bins at a tighter
 * [relativeError].
 *
 * **Update:** O(1) per observation; one `log`/bin-assignment + striped atomic add.
 * `NaN` is dropped, since it has no rank to bin.
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
    /**
     * Smallest magnitude given its own bin. Anything smaller (but non-zero) folds into the
     * bottom bin, so it still counts toward the rank but stops driving bin allocation.
     */
    val minIndexableValue: Double = 1e-9,
    /**
     * Largest magnitude given its own bin. Anything larger folds into the top bin, on the
     * same terms as [minIndexableValue].
     */
    val maxIndexableValue: Double = 1e12,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<SketchResult> {

    init {
        require(relativeError > 0.0 && relativeError < 1.0) {
            "Relative error must be strictly between 0.0 and 1.0; got $relativeError"
        }
        require(minIndexableValue > 0.0) { "minIndexableValue must be positive; got $minIndexableValue" }
        require(maxIndexableValue > minIndexableValue) {
            "maxIndexableValue must exceed minIndexableValue; got $maxIndexableValue and $minIndexableValue"
        }
    }

    private val gamma: Double = (1.0 + relativeError) / (1.0 - relativeError)
    private val multiplier: Double = 1.0 / ln(gamma)

    // Bin indices are clamped to the band spanned by the indexable range. Without this the
    // index is unbounded: the bin array spans min..max observed index and allocates a cell
    // per index in between, so a single denormal next to a single huge value forces an
    // enormous array. Measured before this bound, from exactly two observations: 2.4 MiB at
    // the default relativeError, 28 MiB at 0.001, and 220 MiB at 1e-4. Far enough out the
    // index also overflows Int and the resize is skipped, silently dropping the bins that
    // had already accumulated.
    private val minIndex: Int = ceil(ln(minIndexableValue) * multiplier).toInt()
    private val maxIndex: Int = ceil(ln(maxIndexableValue) * multiplier).toInt()

    private fun indexOf(magnitude: Double): Int = ceil(ln(magnitude) * multiplier).toInt().coerceIn(minIndex, maxIndex)

    private val mode = concurrency.additiveMode()
    private val zeroCount = mode.newDouble(0.0)

    private val positiveBins = ArrayBins(mode)
    private val negativeBins = ArrayBins(mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return
        // NaN has no rank, so it cannot go in a bin. Previously it fell through the sign
        // comparisons into the zero bucket and was silently counted as an observation of
        // zero, which shifts every quantile. LinearHistogramStat already drops it.

        if (value > 0.0) {
            positiveBins.add(indexOf(value), weight)
        } else if (value < 0.0) {
            negativeBins.add(indexOf(-value), weight)
        } else {
            zeroCount.add(weight)
        }
    }

    override fun create(concurrency: Concurrency?) = DDSketchStat(
        relativeError,
        probabilities,
        minIndexableValue,
        maxIndexableValue,
        concurrency ?: this.concurrency,
    )

    override fun merge(values: SketchResult) {
        require(abs(this.gamma - values.gamma) < 1e-9) {
            "Cannot merge DDSketches with different relative error targets"
        }

        zeroCount.add(values.zeroCount)

        // Clamp exactly as indexOf does on the update path. ArrayBins grows a dense array spanning
        // min..max index, and these indices come straight off the wire, so a corrupt or hand-built
        // SketchResult with a matching gamma and an index of ~2e9 would try to allocate that many
        // cells. The update path clamps for precisely this reason.
        values.positiveBins.forEach { (index, weight) ->
            if (weight > 0.0) positiveBins.add(index.coerceIn(minIndex, maxIndex), weight)
        }
        values.negativeBins.forEach { (index, weight) ->
            if (weight > 0.0) negativeBins.add(index.coerceIn(minIndex, maxIndex), weight)
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
            // NaN per quantile rather than 0.0. A zero-filled array is indistinguishable from a
            // sketch that genuinely observed zeros, so an untouched sketch used to render as a
            // p99 of 0.0 - which reads as excellent latency rather than as no data. AucStat
            // already returned NaN in the same situation. Callers who want to branch rather
            // than propagate NaN can check `isEmpty` on the result, which every HasObservationCount
            // gets as an extension.
            computedQuantiles.fill(Double.NaN)
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

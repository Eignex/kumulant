package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stream.ArrayBins
import com.eignex.kumulant.stream.additiveMode
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow

/**
 * DDSketchStat: relative-error quantile sketch with logarithmic bins.
 *
 * Targets [relativeError] for quantile values within the indexable range.
 * Out-of-range magnitudes fold into edge bins and lose that guarantee. Supports
 * negative values via mirrored bins and a zero bucket. At a fixed indexable range,
 * tightening [relativeError] grows bin count roughly as `1/epsilon`.
 *
 * **Use cases:** quantile / percentile monitoring of any positive-skewed
 * scalar (latencies, payload sizes). Reach for this over [TDigestStat] when
 * relative-error guarantees are required.
 *
 * **Memory:** bounded by the indexable range, at
 * `log([maxIndexableValue] / [minIndexableValue]) / log(1 + [relativeError])` bins;
 * roughly 2400 bins at the defaults. Values outside the range fold into the edge
 * bins and still count toward every rank, so the bound holds whatever the input.
 * Narrow the range if you know your data's span and want fewer bins at a tighter
 * [relativeError].
 *
 * **Update:** O(1) per observation; one `log`/bin-assignment + striped atomic add.
 * A `NaN` weight is inert, as everywhere in the library. A `NaN` *value* is a real
 * observation with no bin of its own, so it falls into the zero bucket; filter it
 * upstream with `!X.isNaN()` when that is not wanted.
 *
 * **Concurrency:** Independent additive bins preserve per-cell updates under
 * concurrent modes. [Concurrency.HighWrite] uses striped adders on the JVM;
 * [Concurrency.None] uses plain cells. Multi-bin reads are not atomic, and
 * floating-point results depend on update order.
 */
class DDSketchStat(
    /** Relative value error target within the indexable range. */
    val relativeError: Double = StatDefaults.DD_SKETCH_RELATIVE_ERROR,
    /** Quantiles to evaluate at read time. */
    val probabilities: DoubleArray = StatDefaults.quantileProbabilities(),
    /**
     * Smallest magnitude given its own bin. Anything smaller (but non-zero) folds into the
     * bottom bin, so it still counts toward the rank but stops driving bin allocation.
     */
    val minIndexableValue: Double = StatDefaults.DD_SKETCH_MIN_INDEXABLE_VALUE,
    /**
     * Largest magnitude given its own bin. Anything larger folds into the top bin, on the
     * same terms as [minIndexableValue].
     */
    val maxIndexableValue: Double = StatDefaults.DD_SKETCH_MAX_INDEXABLE_VALUE,
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

    // Clamping bin indices to the indexable range bounds storage across widely separated values.
    private val minIndex: Int = ceil(ln(minIndexableValue) * multiplier).toInt()
    private val maxIndex: Int = ceil(ln(maxIndexableValue) * multiplier).toInt()

    private fun indexOf(magnitude: Double): Int = ceil(ln(magnitude) * multiplier).toInt().coerceIn(minIndex, maxIndex)

    private val mode = concurrency.additiveMode()
    private val zeroCount = mode.newDouble(0.0)

    private val positiveBins = ArrayBins(mode)
    private val negativeBins = ArrayBins(mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return
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
        probabilities.copyOf(),
        minIndexableValue,
        maxIndexableValue,
        concurrency ?: this.concurrency,
    )

    override fun validateMerge(values: SketchResult) {
        require(abs(this.gamma - values.gamma) < PARAMETER_MATCH_TOLERANCE) {
            "Cannot merge DDSketches with different relative error targets"
        }
    }

    override fun merge(values: SketchResult, timestampNanos: Long) {
        validateMerge(values)
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

        // Derive the total from the snapshot rather than from a separate weight cell: any cell
        // updated out of step with the bins lets a concurrent reader see a total the bins do not
        // account for, and walk off the end of the bin list or report all-zero quantiles.
        var total = zeroSnap
        for (w in negSnap.values) total += w
        for (w in posSnap.values) total += w

        if (total <= 0.0) {
            // NaN per quantile rather than 0.0: a zero-filled array is indistinguishable from a
            // sketch that genuinely observed zeros, so an untouched sketch would render a p99 of
            // 0.0, which reads as excellent latency rather than as no data. Callers who want to
            // branch rather than propagate NaN can check `isEmpty` on the result.
            computedQuantiles.fill(Double.NaN)
            return SketchResult(
                probabilities = probabilities.copyOf(),
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
            // Only claim the zero bucket when something actually landed in it; an empty zeroSnap
            // would satisfy `0.0 >= 0.0` and make p0 report 0.0 on all-positive data, a value not
            // in the stream and outside the relative-error contract.
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
            // Copied on the way out, the way BucketHistogramStat copies its edges: the array is a
            // constructor property, so handing the same instance to every snapshot and every replica lets
            // one caller's edit re-label this stat's quantiles and those of every replica in a fan-out.
            probabilities = probabilities.copyOf(),
            quantiles = computedQuantiles,
            gamma = gamma,
            totalWeights = total,
            zeroCount = zeroSnap,
            positiveBins = posSnap,
            negativeBins = negSnap,
        )
    }
}

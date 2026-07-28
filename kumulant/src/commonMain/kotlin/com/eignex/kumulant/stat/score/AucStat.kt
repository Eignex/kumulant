package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * AUC snapshot with the per-bin counts needed for merge. [auc] is `NaN` until at
 * least one positive and one negative have been observed; consult
 * [totalPositives] / [totalNegatives] to detect that case.
 */
@Serializable
@SerialName("AucResult")
data class AucResult(
    /** Estimated ROC area under curve at read time. */
    val auc: Double,
    /** Cumulative positive-label weight observed across the stream. */
    val totalPositives: Double,
    /** Cumulative negative-label weight observed across the stream. */
    val totalNegatives: Double,
    /** Per-bin positive weights, parallel to [negatives]. */
    val positives: DoubleArray,
    /** Per-bin negative weights, parallel to [positives]. */
    val negatives: DoubleArray,
    /** Inclusive lower bound on the score range. */
    val lowerBound: Double,
    /** Inclusive upper bound on the score range. */
    val upperBound: Double,
) : Result {

    /** Number of histogram bins (same length as [positives] / [negatives]). */
    val numBins: Int get() = positives.size

    init {
        require(positives.size == negatives.size) {
            "AucResult positives/negatives arrays must have the same length"
        }
    }

    override fun equals(other: Any?): Boolean = other is AucResult &&
        auc == other.auc &&
        totalPositives == other.totalPositives &&
        totalNegatives == other.totalNegatives &&
        positives.contentEquals(other.positives) &&
        negatives.contentEquals(other.negatives) &&
        lowerBound == other.lowerBound &&
        upperBound == other.upperBound

    override fun hashCode(): Int {
        var h = 1
        h = 31 * h + auc.hashCode()
        h = 31 * h + totalPositives.hashCode()
        h = 31 * h + totalNegatives.hashCode()
        h = 31 * h + positives.contentHashCode()
        h = 31 * h + negatives.contentHashCode()
        h = 31 * h + lowerBound.hashCode()
        h = 31 * h + upperBound.hashCode()
        return h
    }
}

/**
 * Streaming binary ROC-AUC by score-binning. Each update is paired
 * `(score, label)` with `label  in  {0, 1}` (soft labels work too via the
 * convex split into pos/neg weights).
 *
 * Scores are bucketed into [numBins] equal-width buckets across
 * `[lowerBound, upperBound]`; out-of-range scores clamp to the edge bin. Per
 * read the bins are swept high-to-low and AUC is the trapezoidal area under
 * the resulting (FPR, TPR) curve. `O(numBins)` per read, two atomic adds per
 * update.
 *
 * Default range is `[0, 1]`, suitable for calibrated probability scores. For
 * raw classifier margins, pass a wider range or pre-sigmoid the score.
 *
 * **Use cases:** binary classifier discrimination monitoring; ranking
 * quality independent of threshold choice. Pair with [BrierScoreStat] or
 * [LogLossStat] for proper-scoring complements.
 *
 * **Memory:** O([numBins]); two parallel Long arrays for positives and
 * negatives.
 *
 * **Update:** O(1) per paired observation (one atomic add per bin slot).
 * `read()` is O([numBins]) for the trapezoidal sweep.
 *
 * **Concurrency:** Two independent striped atomic adds per update. Lock-free
 * and exact under every [Concurrency] level; bin assignment is
 * deterministic per score and increments commute. The AUC computation at
 * `read()` is a single-threaded sweep of the bin snapshot.
 */
class AucStat(
    /** Number of histogram bins covering `[lowerBound, upperBound]`. */
    val numBins: Int = 256,
    /** Inclusive lower bound on the score range; out-of-range scores clamp to the edge bin. */
    val lowerBound: Double = 0.0,
    /** Inclusive upper bound on the score range; out-of-range scores clamp to the edge bin. */
    val upperBound: Double = 1.0,
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<AucResult> {

    init {
        require(numBins > 0) { "numBins must be > 0; got $numBins" }
        require(upperBound > lowerBound) { "upperBound must be > lowerBound" }
    }

    private val mode = concurrency.additiveMode()
    private val pos: Array<StreamDouble> = Array(numBins) { mode.newDouble(0.0) }
    private val neg: Array<StreamDouble> = Array(numBins) { mode.newDouble(0.0) }
    private val binWidth: Double = (upperBound - lowerBound) / numBins

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight == 0.0) return
        val clamped = x.coerceIn(lowerBound, upperBound)
        val bin = ((clamped - lowerBound) / binWidth).toInt().coerceIn(0, numBins - 1)
        val posWeight = y * weight
        val negWeight = (1.0 - y) * weight
        if (posWeight != 0.0) pos[bin].add(posWeight)
        if (negWeight != 0.0) neg[bin].add(negWeight)
    }

    override fun read(timestampNanos: Long): AucResult {
        // The totals are derived from the bin snapshot rather than tracked in their own
        // cells. Separate total cells could be read out of step with the bins under a
        // concurrent update, letting the cumulative count exceed the total and pushing
        // tpr above 1.0 and the reported area above 1.0. Summing the snapshot makes the
        // result self-consistent by construction, at whatever staleness the snapshot has.
        val posSnap = DoubleArray(numBins) { pos[it].load() }
        val negSnap = DoubleArray(numBins) { neg[it].load() }
        var tp = 0.0
        var tn = 0.0
        for (b in 0 until numBins) {
            tp += posSnap[b]
            tn += negSnap[b]
        }
        if (tp <= 0.0 || tn <= 0.0) {
            return AucResult(Double.NaN, tp, tn, posSnap, negSnap, lowerBound, upperBound)
        }
        var cumPos = 0.0
        var cumNeg = 0.0
        var area = 0.0
        var prevFpr = 0.0
        var prevTpr = 0.0
        for (b in numBins - 1 downTo 0) {
            cumPos += posSnap[b]
            cumNeg += negSnap[b]
            val tpr = cumPos / tp
            val fpr = cumNeg / tn
            area += (fpr - prevFpr) * (tpr + prevTpr) / 2.0
            prevFpr = fpr
            prevTpr = tpr
        }
        return AucResult(area, tp, tn, posSnap, negSnap, lowerBound, upperBound)
    }

    override fun merge(values: AucResult) {
        require(values.numBins == numBins) {
            "AUC merge bin count mismatch: this=$numBins, other=${values.numBins}"
        }
        require(values.lowerBound == lowerBound && values.upperBound == upperBound) {
            "AUC merge range mismatch: this=[$lowerBound,$upperBound], " +
                "other=[${values.lowerBound},${values.upperBound}]"
        }
        // Only the bins are folded in; the incoming totals are the sum of its own bins,
        // so they are reconstructed on the next read.
        for (i in 0 until numBins) {
            pos[i].add(values.positives[i])
            neg[i].add(values.negatives[i])
        }
    }

    override fun reset() {
        for (i in 0 until numBins) {
            pos[i].store(0.0)
            neg[i].store(0.0)
        }
    }

    override fun create(concurrency: Concurrency?): AucStat =
        AucStat(numBins, lowerBound, upperBound, concurrency ?: this.concurrency)
}

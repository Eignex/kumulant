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
@SerialName("AucStat")
data class AucResult(
    val auc: Double,
    val totalPositives: Double,
    val totalNegatives: Double,
    val positives: DoubleArray,
    val negatives: DoubleArray,
    val lowerBound: Double,
    val upperBound: Double,
) : Result {

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
 * `(score, label)` with `label ∈ {0, 1}` (soft labels work too via the
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
 */
class AucStat(
    val numBins: Int = 256,
    val lowerBound: Double = 0.0,
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
    private val totalPos = mode.newDouble(0.0)
    private val totalNeg = mode.newDouble(0.0)
    private val binWidth: Double = (upperBound - lowerBound) / numBins

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight == 0.0) return
        val clamped = x.coerceIn(lowerBound, upperBound)
        val bin = ((clamped - lowerBound) / binWidth).toInt().coerceIn(0, numBins - 1)
        val posWeight = y * weight
        val negWeight = (1.0 - y) * weight
        if (posWeight != 0.0) {
            pos[bin].add(posWeight)
            totalPos.add(posWeight)
        }
        if (negWeight != 0.0) {
            neg[bin].add(negWeight)
            totalNeg.add(negWeight)
        }
    }

    override fun read(timestampNanos: Long): AucResult {
        val tp = totalPos.load()
        val tn = totalNeg.load()
        val posSnap = DoubleArray(numBins) { pos[it].load() }
        val negSnap = DoubleArray(numBins) { neg[it].load() }
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
        for (i in 0 until numBins) {
            pos[i].add(values.positives[i])
            neg[i].add(values.negatives[i])
        }
        totalPos.add(values.totalPositives)
        totalNeg.add(values.totalNegatives)
    }

    override fun reset() {
        for (i in 0 until numBins) {
            pos[i].store(0.0)
            neg[i].store(0.0)
        }
        totalPos.store(0.0)
        totalNeg.store(0.0)
    }

    override fun create(concurrency: Concurrency?): AucStat =
        AucStat(numBins, lowerBound, upperBound, concurrency ?: this.concurrency)
}

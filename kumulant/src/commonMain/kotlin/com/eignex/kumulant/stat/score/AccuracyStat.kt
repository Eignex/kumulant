package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/**
 * Streaming classification accuracy: paired `(predictedClass, trueClass)` aggregated as the weighted
 * mean of `1[predicted == truth]`.
 *
 * Labels are resolved by [asClassLabel], the same rule [ConfusionMatrixStat] applies, so a pair either
 * names two classes in `[0, numClasses)` or is ignored entirely.
 *
 * That agreement is why [numClasses] is required: [ConfusionMatrixResult.accuracy] exists so a caller can
 * cross-check the two, which only means anything if both bound the labels identically.
 *
 * For the full P/R/F1 surface and a per-class breakdown reach for [ConfusionMatrixStat]; this stat is the
 * O(1)-memory shortcut when only the scalar accuracy is needed.
 *
 * **Memory:** O(1); backed by a [MeanStat]. [numClasses] only bounds the labels, it never allocates -
 * that is the saving over [ConfusionMatrixStat]'s `numClasses^2` cells.
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [MeanStat]'s concurrency model.
 */
class AccuracyStat(
    /** Number of classes; class indices are `[0, numClasses)`. Pairs naming anything else are ignored. */
    val numClasses: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<WeightedMeanResult> {

    init {
        require(numClasses > 0) { "numClasses must be > 0; got $numClasses" }
    }

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        // No separate NaN guard: asClassLabel already rejects NaN, because `NaN.toInt()` is 0 and
        // `0.0 == NaN` is false, so the round-trip check fails. ConfusionMatrixStat carried a redundant
        // one for the same reason.
        val predicted = x.asClassLabel(numClasses)
        val truth = y.asClassLabel(numClasses)
        if (predicted < 0 || truth < 0) return

        val match = if (predicted == truth) 1.0 else 0.0
        inner.update(match, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = AccuracyStat(numClasses, concurrency ?: this.concurrency)
}

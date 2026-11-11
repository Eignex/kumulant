package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Accuracy
import com.eignex.kumulant.stat.summary.MeanStat

/**
 * Streaming classification accuracy: paired `(predictedClass, trueClass)` aggregated as the weighted
 * mean of `1[predicted == truth]`.
 *
 * Labels follow the [asClassLabel] rule used by [ConfusionMatrixStat], so a pair either
 * names two classes in `[0, numClasses)` or is ignored entirely.
 *
 * That agreement is why [numClasses] is required: [ConfusionMatrixResult.accuracy] exists so a caller can
 * cross-check the two, which only means anything if both bound the labels identically.
 *
 * **Use cases:** scalar accuracy monitoring for a classifier. Reach for [ConfusionMatrixStat] when the
 * full P/R/F1 surface or a per-class breakdown is needed; this stat is the O(1)-memory shortcut.
 *
 * **Memory:** O(1); backed by a [MeanStat]. [numClasses] only bounds the labels, it never allocates -
 * that is the saving over [ConfusionMatrixStat]'s `numClasses^2` cells.
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [MeanStat]'s concurrency model.
 */
@Suppress("FunctionNaming")
fun AccuracyStat(numClasses: Int, concurrency: Concurrency = Concurrency.None): PairedStat<AccuracyResult> =
    Accuracy(numClasses).materialize(concurrency)

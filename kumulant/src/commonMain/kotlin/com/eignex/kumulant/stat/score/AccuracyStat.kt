package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/**
 * Streaming classification accuracy: paired `(predictedClass, trueClass)`
 * aggregated as the weighted mean of `1[predicted == truth]`. Classes are
 * compared on `toLong()` so floating-point class indices round-trip safely.
 *
 * For the full P/R/F1 surface and a per-class breakdown reach for
 * [ConfusionMatrixStat]; this stat is the O(1)-memory shortcut when only the
 * scalar accuracy is needed.
 *
 * **Memory:** O(1); backed by a [MeanStat].
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [MeanStat]'s concurrency model.
 */
class AccuracyStat(override val concurrency: Concurrency = Concurrency.None) : PairedStat<WeightedMeanResult> {

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val match = if (x.toLong() == y.toLong()) 1.0 else 0.0
        inner.update(match, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = AccuracyStat(concurrency ?: this.concurrency)
}

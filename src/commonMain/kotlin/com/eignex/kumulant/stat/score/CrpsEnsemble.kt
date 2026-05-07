package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.math.abs

/**
 * Streaming mean CRPS for ensemble forecasts. The (samples, observation)
 * input shape doesn't fit any modality (ensemble size varies per row), so
 * this is a raw [Stat] with a typed update method — same pattern as the tree
 * histograms.
 *
 * Per-update cost is dominated by sorting the samples (`O(m log m)`); the
 * pairwise-distance term uses the sorted-rank identity
 * `Σᵢⱼ |xᵢ − xⱼ| = 2·Σₖ k·(m − k)·(x_(k+1) − x_(k))` to evaluate in `O(m)`
 * after the sort. Aggregation itself is a [Mean] and adds negligible overhead.
 */
class CrpsEnsemble(
    override val concurrency: Concurrency = Concurrency.None,
) : Stat<WeightedMeanResult> {

    private val inner = Mean(concurrency)

    /**
     * Score [samples] against observation [y] via the standard ensemble CRPS
     * estimator `(1/m)·Σ|xᵢ − y| − 1/(2m²)·Σᵢⱼ|xᵢ − xⱼ|`. The samples array
     * is copied internally before sorting so caller-side mutation doesn't
     * affect aggregation.
     */
    fun update(samples: DoubleArray, y: Double, weight: Double = 1.0) {
        inner.update(ensembleCrps(samples, y), weight = weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = CrpsEnsemble(concurrency ?: this.concurrency)
}

private fun ensembleCrps(samples: DoubleArray, y: Double): Double {
    val m = samples.size
    if (m == 0) return Double.NaN
    val sorted = samples.copyOf().also { it.sort() }
    var meanAbs = 0.0
    for (x in sorted) meanAbs += abs(x - y)
    meanAbs /= m
    var pairSum = 0.0
    for (k in 0 until m - 1) {
        pairSum += (k + 1).toLong() * (m - k - 1).toLong() * (sorted[k + 1] - sorted[k])
    }
    val pairTerm = pairSum / (m.toLong() * m)
    return meanAbs - pairTerm
}

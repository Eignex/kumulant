package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Per-bin sufficient statistics for histogram-based gradient boosting:
 * `(Σ wᵢ·gᵢ, Σ wᵢ·hᵢ, Σ wᵢ)` indexed by bin.
 *
 * Downstream split-finding sweeps the arrays left-to-right with prefix sums
 * to compute gain at each candidate split point.
 */
@Serializable
@SerialName("GradientHistogram")
data class GradientHistogramResult(
    val sumG: DoubleArray,
    val sumH: DoubleArray,
    val totalWeights: DoubleArray,
) : Result {
    val numBins: Int get() = sumG.size

    init {
        require(sumG.size == sumH.size && sumG.size == totalWeights.size) {
            "GradientHistogramResult arrays must have the same length"
        }
    }

    override fun equals(other: Any?): Boolean = other is GradientHistogramResult &&
        sumG.contentEquals(other.sumG) &&
        sumH.contentEquals(other.sumH) &&
        totalWeights.contentEquals(other.totalWeights)

    override fun hashCode(): Int =
        31 * (31 * sumG.contentHashCode() + sumH.contentHashCode()) + totalWeights.contentHashCode()
}

/**
 * Per-bin gradient/hessian/weight accumulator for histogram-based GBM splits.
 *
 * Pre-binned input: callers map feature value → integer bin once at training
 * start, then pass that index here. Three independent atomic adds per update;
 * additive category, no lock.
 */
class GradientHistogram(
    val numBins: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : Stat<GradientHistogramResult> {

    init { require(numBins > 0) { "numBins must be > 0; got $numBins" } }

    private val mode = concurrency.additiveMode()
    private val sumG: Array<StreamDouble> = Array(numBins) { mode.newDouble(0.0) }
    private val sumH: Array<StreamDouble> = Array(numBins) { mode.newDouble(0.0) }
    private val sumW: Array<StreamDouble> = Array(numBins) { mode.newDouble(0.0) }

    fun update(binIndex: Int, gradient: Double, hessian: Double, weight: Double = 1.0) {
        require(binIndex in 0 until numBins) {
            "binIndex $binIndex outside [0, $numBins)"
        }
        if (weight == 0.0) return
        sumG[binIndex].add(gradient * weight)
        sumH[binIndex].add(hessian * weight)
        sumW[binIndex].add(weight)
    }

    override fun read(timestampNanos: Long) = GradientHistogramResult(
        DoubleArray(numBins) { sumG[it].load() },
        DoubleArray(numBins) { sumH[it].load() },
        DoubleArray(numBins) { sumW[it].load() },
    )

    override fun merge(values: GradientHistogramResult) {
        require(values.numBins == numBins) {
            "numBins mismatch on merge: this=$numBins, other=${values.numBins}"
        }
        for (i in 0 until numBins) {
            sumG[i].add(values.sumG[i])
            sumH[i].add(values.sumH[i])
            sumW[i].add(values.totalWeights[i])
        }
    }

    override fun reset() {
        for (i in 0 until numBins) {
            sumG[i].store(0.0)
            sumH[i].store(0.0)
            sumW[i].store(0.0)
        }
    }

    override fun create(concurrency: Concurrency?) =
        GradientHistogram(numBins, concurrency ?: this.concurrency)
}

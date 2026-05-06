package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Per-feature × per-bin sufficient statistics for histogram-based gradient
 * boosting. Row-major layout: index `f * numBins + b` holds the cell for
 * feature `f`, bin `b`.
 */
@Serializable
@SerialName("MultiGradientHistogram")
data class MultiGradientHistogramResult(
    val numFeatures: Int,
    val numBins: Int,
    val sumG: DoubleArray,
    val sumH: DoubleArray,
    val totalWeights: DoubleArray,
) : Result {

    init {
        val expected = numFeatures * numBins
        require(sumG.size == expected && sumH.size == expected && totalWeights.size == expected) {
            "MultiGradientHistogramResult arrays must have size numFeatures*numBins=$expected"
        }
    }

    fun sumG(feature: Int, bin: Int): Double = sumG[feature * numBins + bin]
    fun sumH(feature: Int, bin: Int): Double = sumH[feature * numBins + bin]
    fun totalWeight(feature: Int, bin: Int): Double = totalWeights[feature * numBins + bin]

    /** Slice this multi-feature histogram into a single-feature [GradientHistogramResult]. */
    fun forFeature(feature: Int): GradientHistogramResult {
        require(feature in 0 until numFeatures) {
            "feature $feature outside [0, $numFeatures)"
        }
        val base = feature * numBins
        val g = DoubleArray(numBins) { sumG[base + it] }
        val h = DoubleArray(numBins) { sumH[base + it] }
        val w = DoubleArray(numBins) { totalWeights[base + it] }
        return GradientHistogramResult(g, h, w)
    }

    override fun equals(other: Any?): Boolean = other is MultiGradientHistogramResult &&
        numFeatures == other.numFeatures &&
        numBins == other.numBins &&
        sumG.contentEquals(other.sumG) &&
        sumH.contentEquals(other.sumH) &&
        totalWeights.contentEquals(other.totalWeights)

    override fun hashCode(): Int {
        var h = 1
        h = 31 * h + numFeatures
        h = 31 * h + numBins
        h = 31 * h + sumG.contentHashCode()
        h = 31 * h + sumH.contentHashCode()
        h = 31 * h + totalWeights.contentHashCode()
        return h
    }
}

/**
 * Multi-feature gradient histogram: one row per `(feature, bin)` pair holding
 * `(Σ wg, Σ wh, Σ w)`. Single-row update accepts pre-binned feature indices
 * (canonical XGBoost/LightGBM input), avoiding numFeatures separate stat calls
 * per row. Three atomic adds per `(feature, bin)` cell on the hot path.
 */
class MultiGradientHistogram(
    val numFeatures: Int,
    val numBins: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : Stat<MultiGradientHistogramResult> {

    init {
        require(numFeatures > 0) { "numFeatures must be > 0; got $numFeatures" }
        require(numBins > 0) { "numBins must be > 0; got $numBins" }
    }

    private val size = numFeatures * numBins
    private val mode = concurrency.additiveMode()
    private val sumG: Array<StreamDouble> = Array(size) { mode.newDouble(0.0) }
    private val sumH: Array<StreamDouble> = Array(size) { mode.newDouble(0.0) }
    private val sumW: Array<StreamDouble> = Array(size) { mode.newDouble(0.0) }

    /**
     * Update all features for a single row. [featureBins] holds the pre-binned
     * integer index for each feature; [gradient] / [hessian] / [weight] are
     * shared across the row.
     */
    fun update(featureBins: IntArray, gradient: Double, hessian: Double, weight: Double = 1.0) {
        require(featureBins.size == numFeatures) {
            "featureBins size ${featureBins.size} != numFeatures $numFeatures"
        }
        if (weight == 0.0) return
        val gw = gradient * weight
        val hw = hessian * weight
        for (f in 0 until numFeatures) {
            val bin = featureBins[f]
            require(bin in 0 until numBins) {
                "feature $f bin $bin outside [0, $numBins)"
            }
            val idx = f * numBins + bin
            sumG[idx].add(gw)
            sumH[idx].add(hw)
            sumW[idx].add(weight)
        }
    }

    override fun read(timestampNanos: Long) = MultiGradientHistogramResult(
        numFeatures,
        numBins,
        DoubleArray(size) { sumG[it].load() },
        DoubleArray(size) { sumH[it].load() },
        DoubleArray(size) { sumW[it].load() },
    )

    override fun merge(values: MultiGradientHistogramResult) {
        require(values.numFeatures == numFeatures && values.numBins == numBins) {
            "Shape mismatch on merge: this=($numFeatures,$numBins), " +
                "other=(${values.numFeatures},${values.numBins})"
        }
        for (i in 0 until size) {
            sumG[i].add(values.sumG[i])
            sumH[i].add(values.sumH[i])
            sumW[i].add(values.totalWeights[i])
        }
    }

    override fun reset() {
        for (i in 0 until size) {
            sumG[i].store(0.0)
            sumH[i].store(0.0)
            sumW[i].store(0.0)
        }
    }

    override fun create(concurrency: Concurrency?) =
        MultiGradientHistogram(numFeatures, numBins, concurrency ?: this.concurrency)
}

package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs

/**
 * Per-bin reliability snapshot for a binary probabilistic classifier.
 * Bins are equal-width over `[0, 1]` indexed by predicted probability.
 * Underlying sums use weights so soft labels and importance-weighted streams
 * compose correctly.
 */
@Serializable
@SerialName("ReliabilityResult")
data class ReliabilityResult(
    /** Number of equal-width probability bins covering `[0, 1]`. */
    val numBins: Int,
    /** `Sum w_i*prob_i` per bin. */
    val sumProbability: DoubleArray,
    /** `Sum w_i*outcome_i` per bin. */
    val sumOutcome: DoubleArray,
    /** `Sum w_i` per bin. */
    val totalWeights: DoubleArray,
) : Result {

    init {
        require(sumProbability.size == numBins && sumOutcome.size == numBins && totalWeights.size == numBins) {
            "ReliabilityResult arrays must have length numBins=$numBins"
        }
    }

    /** Mean predicted probability per bin (NaN where the bin is empty). */
    val meanProbability: DoubleArray get() = DoubleArray(numBins) { i ->
        if (totalWeights[i] > 0.0) sumProbability[i] / totalWeights[i] else Double.NaN
    }

    /** Empirical outcome rate per bin (NaN where the bin is empty). */
    val outcomeRate: DoubleArray get() = DoubleArray(numBins) { i ->
        if (totalWeights[i] > 0.0) sumOutcome[i] / totalWeights[i] else Double.NaN
    }

    /**
     * Expected Calibration Error: weighted mean over bins of
     * `|meanProbability[i] - outcomeRate[i]|`. Empty bins contribute 0.
     */
    fun expectedCalibrationError(): Double {
        var totalW = 0.0
        for (w in totalWeights) totalW += w
        if (totalW <= 0.0) return 0.0
        var ece = 0.0
        for (i in 0 until numBins) {
            val w = totalWeights[i]
            if (w <= 0.0) continue
            val gap = abs(sumProbability[i] / w - sumOutcome[i] / w)
            ece += (w / totalW) * gap
        }
        return ece
    }

    override fun equals(other: Any?): Boolean = other is ReliabilityResult &&
        numBins == other.numBins &&
        sumProbability.contentEquals(other.sumProbability) &&
        sumOutcome.contentEquals(other.sumOutcome) &&
        totalWeights.contentEquals(other.totalWeights)

    override fun hashCode(): Int = 31 * (
        31 * (31 * numBins + sumProbability.contentHashCode()) +
            sumOutcome.contentHashCode()
        ) + totalWeights.contentHashCode()
}

/**
 * Reliability diagram primitive for binary probabilistic forecasts. Paired
 * input is `(predictedProbability, outcome)`; predictions are bucketed into
 * [numBins] equal-width bins across `[0, 1]`. Outcomes are typically `{0, 1}`
 * but soft labels and weighted updates work uniformly.
 *
 * Predictions outside `[0, 1]` are clamped to the nearest edge bin.
 *
 * **Use cases:** calibration diagnostics for probabilistic forecasters — the
 * raw material for reliability diagrams and Expected Calibration Error.
 * Pair with [BrierScoreStat] for the matching proper-scoring number.
 *
 * **Memory:** O([numBins]) — three parallel Double arrays per bin.
 *
 * **Update:** O(1) per paired observation (three atomic adds on the
 * destination bin).
 *
 * **Concurrency:** Three independent striped atomic adds per update.
 * Lock-free and exact under every [Concurrency] level — bin assignment is
 * deterministic per prediction and increments commute.
 */
class ReliabilityStat(val numBins: Int, override val concurrency: Concurrency = Concurrency.None) :
    PairedStat<ReliabilityResult> {

    init {
        require(numBins > 0) { "numBins must be > 0; got $numBins" }
    }

    private val mode = concurrency.additiveMode()
    private val sumP: Array<StreamDouble> = Array(numBins) { mode.newDouble(0.0) }
    private val sumO: Array<StreamDouble> = Array(numBins) { mode.newDouble(0.0) }
    private val sumW: Array<StreamDouble> = Array(numBins) { mode.newDouble(0.0) }

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight == 0.0) return
        val clamped = x.coerceIn(0.0, 1.0)
        val bin = (clamped * numBins).toInt().coerceIn(0, numBins - 1)
        sumP[bin].add(clamped * weight)
        sumO[bin].add(y * weight)
        sumW[bin].add(weight)
    }

    override fun read(timestampNanos: Long) = ReliabilityResult(
        numBins,
        DoubleArray(numBins) { sumP[it].load() },
        DoubleArray(numBins) { sumO[it].load() },
        DoubleArray(numBins) { sumW[it].load() },
    )

    override fun merge(values: ReliabilityResult) {
        require(values.numBins == numBins) {
            "numBins mismatch on merge: this=$numBins, other=${values.numBins}"
        }
        for (i in 0 until numBins) {
            sumP[i].add(values.sumProbability[i])
            sumO[i].add(values.sumOutcome[i])
            sumW[i].add(values.totalWeights[i])
        }
    }

    override fun reset() {
        for (i in 0 until numBins) {
            sumP[i].store(0.0)
            sumO[i].store(0.0)
            sumW[i].store(0.0)
        }
    }

    override fun create(concurrency: Concurrency?) = ReliabilityStat(numBins, concurrency ?: this.concurrency)
}

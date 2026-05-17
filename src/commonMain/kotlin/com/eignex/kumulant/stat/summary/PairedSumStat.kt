package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Paired weighted-sum snapshot: `Sum w_i*x_i`, `Sum w_i*y_i`, and `Sum w_i`.
 *
 * The canonical use case is histogram-based gradient boosting, where each leaf
 * accumulates `(Sumg, Sumh, n)` to score splits.
 */
@Serializable
@SerialName("PairedSumResult")
data class PairedSumResult(
    val totalWeights: Double,
    val sumX: Double,
    val sumY: Double,
) : Result

/**
 * Weighted paired sum `(Sum w_i*x_i, Sum w_i*y_i)` with accumulated weight.
 *
 * Three independent atomic adds - additive category, no lock. Use as a
 * primitive for gradient/hessian aggregation in boosting and similar
 * paired-flow accumulators.
 *
 * Numerical caveat matches [SumStat]: very long mixed-magnitude streams accumulate
 * ulp drift on the order of sqrtn. For compensated accumulation, consider running
 * a Welford-style stat over each axis instead.
 */
class PairedSumStat(
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<PairedSumResult> {

    private val mode = concurrency.additiveMode()
    private val totalWeights = mode.newDouble(0.0)
    private val sumX = mode.newDouble(0.0)
    private val sumY = mode.newDouble(0.0)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight == 0.0) return
        totalWeights.add(weight)
        sumX.add(x * weight)
        sumY.add(y * weight)
    }

    override fun read(timestampNanos: Long) =
        PairedSumResult(totalWeights.load(), sumX.load(), sumY.load())

    override fun merge(values: PairedSumResult) {
        totalWeights.add(values.totalWeights)
        sumX.add(values.sumX)
        sumY.add(values.sumY)
    }

    override fun reset() {
        totalWeights.store(0.0)
        sumX.store(0.0)
        sumY.store(0.0)
    }

    override fun create(concurrency: Concurrency?) = PairedSumStat(concurrency ?: this.concurrency)
}

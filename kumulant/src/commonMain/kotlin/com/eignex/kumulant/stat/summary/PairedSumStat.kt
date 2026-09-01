package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.isInertWeight
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
    /** Cumulative observation weight (`Sum w_i`). */
    override val totalWeights: Double,
    /** Weighted `x` sum (`Sum w_i * x_i`). */
    val sumX: Double,
    /** Weighted `y` sum (`Sum w_i * y_i`). */
    val sumY: Double,
) : HasObservationCount

/**
 * Weighted paired sum `(Sum w_i*x_i, Sum w_i*y_i)` with accumulated weight.
 *
 * Numerical caveat matches [SumStat]: very long mixed-magnitude streams
 * accumulate ulp drift on the order of `sqrt(n)`. For compensated accumulation,
 * consider running a Welford-style stat over each axis instead.
 *
 * **Use cases:** gradient/Hessian aggregation in histogram boosting (one leaf
 * accumulates `(Sum g, Sum h, n)`), or any two-axis additive flow.
 *
 * **Memory:** O(1); three double cells.
 *
 * **Update:** O(1) per observation (three atomic adds).
 *
 * **Concurrency:** Three independent atomic adds per update; exact under
 * every [Concurrency] level. A `read()` interleaved between the writes of a
 * single update can briefly observe partially-applied state, but the per-cell
 * guarantees hold. [Concurrency.HighWrite] switches the cells to striped
 * adders.
 */
class PairedSumStat(override val concurrency: Concurrency = Concurrency.None) : PairedStat<PairedSumResult> {

    private val mode = concurrency.additiveMode()
    private val totalWeights = mode.newDouble(0.0)
    private val sumX = mode.newDouble(0.0)
    private val sumY = mode.newDouble(0.0)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        totalWeights.add(weight)
        sumX.add(x * weight)
        sumY.add(y * weight)
    }

    override fun read(timestampNanos: Long) = PairedSumResult(totalWeights.load(), sumX.load(), sumY.load())

    override fun merge(values: PairedSumResult, workspace: com.eignex.koblas.Workspace?) {
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

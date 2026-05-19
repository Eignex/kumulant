package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Weighted sum snapshot. */
@Serializable
@SerialName("SumResult")
data class SumResult(
    /** Weighted running sum. */
    val sum: Double,
) : Result

/**
 * Weighted sum `Sum value*weight` over the stream.
 *
 * Uses naive accumulation, so very long streams of mixed-magnitude values can
 * accumulate ulp drift on the order of `sqrt(n)`. For compensated floating-point
 * accumulation, prefer [MeanStat] or [VarianceStat] (Welford recurrences) and
 * recover the sum as `mean * totalWeights`.
 *
 * **Use cases:** totals, counters, gradient aggregation — the primitive
 * additive accumulator.
 *
 * **Memory:** O(1) — a single double cell.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Single atomic add per update — exact under every
 * [Concurrency] level. [Concurrency.HighWrite] switches the cell to a striped
 * adder (`DoubleAdder` on JVM) for higher throughput under heavy write
 * contention, trading a slower `read()` that sweeps all stripes.
 */
class SumStat(
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<SumResult> {

    private val mode = concurrency.additiveMode()
    private val value = mode.newDouble(0.0)

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        this.value.add(value * weight)
    }

    override fun read(timestampNanos: Long) = SumResult(value.load())

    override fun merge(values: SumResult) {
        value.add(values.sum)
    }

    override fun reset() {
        value.store(0.0)
    }

    override fun create(concurrency: Concurrency?) = SumStat(concurrency ?: this.concurrency)
}

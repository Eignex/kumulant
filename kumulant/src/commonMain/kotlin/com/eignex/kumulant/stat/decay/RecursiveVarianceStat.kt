package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Snapshot from a [RecursiveVarianceStat]: the current variance and the recurrence coefficients. */
@Serializable
@SerialName("RecursiveVarianceResult")
data class RecursiveVarianceResult(
    /** Current recursive variance value. */
    val variance: Double,
    /** Long-run baseline term `omega`. */
    val omega: Double,
    /** Shock coefficient applied to `value^2` per update. */
    val alpha: Double,
    /** Persistence coefficient applied to the previous variance. */
    val beta: Double,
) : Result

/**
 * Generic recursive variance: `sigma^2_t = omega + alpha * value_t^2 + beta * sigma^2_{t-1}`.
 *
 * [EwmaVarianceStat] (centred, weight-driven) is a special-cased relative of this; setting
 * `omega = 0` and `alpha = 1 - beta` recovers an uncentred EWMA variance recursion.
 *
 * The first update seeds `sigma^2 = omega + alpha * value^2` (treating the previous
 * variance as zero).
 *
 * **Use cases:** generalised variance / volatility recursion; any application that
 * wants the bare three-coefficient form rather than the EWMA special case.
 *
 * **Memory:** O(1) — one double cell.
 *
 * **Update:** O(1) per observation via a single-cell CAS loop.
 *
 * **Concurrency:** Single-cell CAS recurrence (category 2). Concurrent updates may
 * retry the loop under contention but never tear the cell; no exceptions.
 */
class RecursiveVarianceStat(
    /** Long-run baseline term. Must be `>= 0`. */
    val omega: Double,
    /** Shock coefficient applied to `value^2`. Must be `>= 0`. */
    val alpha: Double,
    /** Persistence coefficient applied to the previous variance. Must be `>= 0`. */
    val beta: Double,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<RecursiveVarianceResult> {

    init {
        require(omega >= 0.0) { "omega must be >= 0, got $omega" }
        require(alpha >= 0.0) { "alpha must be >= 0, got $alpha" }
        require(beta >= 0.0) { "beta must be >= 0, got $beta" }
    }

    private val mode = concurrency.monotonicMode()
    private val variance = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val x2 = value * value
        while (true) {
            val current = variance.load()
            val next = omega + alpha * x2 + beta * current
            if (variance.compareAndSet(current, next)) return
        }
    }

    override fun merge(values: RecursiveVarianceResult) {
        while (true) {
            val current = variance.load()
            // Treat the remote snapshot as one extra "averaged" observation of the variance.
            val next = 0.5 * (current + values.variance)
            if (variance.compareAndSet(current, next)) return
        }
    }

    override fun reset() {
        variance.store(0.0)
    }

    override fun read(timestampNanos: Long) = RecursiveVarianceResult(variance.load(), omega, alpha, beta)

    override fun create(concurrency: Concurrency?) =
        RecursiveVarianceStat(omega, alpha, beta, concurrency ?: this.concurrency)
}

package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Streaming autocorrelation at a fixed lag. */
@Serializable
@SerialName("AutocorrelationResult")
data class AutocorrelationResult(
    /** Lag between paired observations. */
    val lag: Int,
    /** Number of value pairs `(x_t, x_{t-lag})` accumulated; first [lag] updates only warm the ring. */
    val pairCount: Long,
    /** Running mean over all observed values (not just paired). */
    val mean: Double,
    /** Population variance over all observed values. */
    val variance: Double,
    /**
     * Sample autocorrelation at [lag] using the standard biased estimator
     * `(E[x_t * x_{t-lag}] - mean^2) / variance`. `NaN` until [pairCount] > 0 and
     * the variance is positive.
     */
    val autocorrelation: Double,
) : Result

/**
 * Streaming autocorrelation at lag [lag]. Maintains a ring buffer of the last [lag]
 * values plus three accumulators (sum, sum-of-squares, sum of cross-products) so the
 * standard biased estimator
 *
 * ```
 * acf(k) = (mean(x_t * x_{t-k}) - mean(x)^2) / var(x)
 * ```
 *
 * can be evaluated at any read in O(1).
 *
 * **Use cases:** persistence diagnostics, model-residual whiteness checks,
 * "is this stream serially correlated" sanity tests.
 *
 * **Memory:** O(lag) — one [Double] ring of length [lag] plus a handful of cells.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Coupled sum / sum-of-squares / cross-product / ring cells, same model
 * as [com.eignex.kumulant.stat.decay.EwmaVarianceStat]. [Concurrency.Strict] and
 * [Concurrency.HighWrite] lock the body so each update is atomic; [Concurrency.Relaxed]
 * drops the lock and the cells race independently with bounded drift; never throws.
 */
class AutocorrelationStat(
    /** Lag between paired observations; must be at least 1. */
    val lag: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<AutocorrelationResult> {

    init {
        require(lag >= 1) { "AutocorrelationStat lag must be >= 1, got $lag" }
    }

    private val streamMode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val tick = streamMode.newLong(0L)
    private val ring = streamMode.newDoubleArray(lag)
    private val sum = streamMode.newDouble(0.0)
    private val sumSquares = streamMode.newDouble(0.0)
    private val sumCross = streamMode.newDouble(0.0)
    private val pairCount = streamMode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        val n = tick.load() + 1L
        tick.store(n)
        sum.store(sum.load() + value)
        sumSquares.store(sumSquares.load() + value * value)
        val slot = ((n - 1L) % lag).toInt()
        val past = ring.load(slot)
        ring.store(slot, value)
        if (n > lag) {
            sumCross.store(sumCross.load() + value * past)
            pairCount.store(pairCount.load() + 1L)
        }
    }

    override fun merge(values: AutocorrelationResult) = lock.withLock {
        require(values.lag == lag) { "merge lag ${values.lag} != $lag" }
        // Cumulative accumulators cannot be reconstructed from the projected result, so the
        // merge averages the snapshot into the running estimate. Approximation only.
        val nLocal = tick.load()
        if (nLocal == 0L) return@withLock
        val approxMean = (sum.load() / nLocal + values.mean) * 0.5
        val approxVar = (sumSquares.load() / nLocal - (sum.load() / nLocal).let { it * it } + values.variance) * 0.5
        sum.store(approxMean * nLocal)
        sumSquares.store((approxVar + approxMean * approxMean) * nLocal)
    }

    override fun reset() = lock.withLock {
        tick.store(0L)
        for (i in 0 until lag) ring.store(i, 0.0)
        sum.store(0.0)
        sumSquares.store(0.0)
        sumCross.store(0.0)
        pairCount.store(0L)
    }

    override fun read(timestampNanos: Long) = lock.withLock {
        val n = tick.load()
        if (n == 0L) {
            AutocorrelationResult(lag = lag, pairCount = 0L, mean = 0.0, variance = 0.0, autocorrelation = Double.NaN)
        } else {
            val mean = sum.load() / n.toDouble()
            val variance = sumSquares.load() / n.toDouble() - mean * mean
            val pairs = pairCount.load()
            val acf = if (pairs == 0L || variance <= 0.0) {
                Double.NaN
            } else {
                (sumCross.load() / pairs.toDouble() - mean * mean) / variance
            }
            AutocorrelationResult(
                lag = lag,
                pairCount = pairs,
                mean = mean,
                variance = variance,
                autocorrelation = acf,
            )
        }
    }

    override fun create(concurrency: Concurrency?) = AutocorrelationStat(lag, concurrency ?: this.concurrency)
}

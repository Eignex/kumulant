package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode

/**
 * Frugal-streaming single-quantile estimator.
 *
 * Keeps one `Double` of state that drifts toward the target quantile [q]; the drift
 * magnitude is scaled by [stepSize]. Cheap and memory-flat but biased and noisy -
 * use [DDSketchStat] when accuracy matters.
 *
 * Treats observations as unweighted (one unit step per update regardless of weight);
 * scaling the step by raw weight would let a single high-weight observation overshoot
 * the target catastrophically. [merge] averages two estimates as a coarse approximation
 * — frugal sketches do not admit a true associative combine.
 *
 * # Concurrency
 *
 * The recurrence walks the estimate one step at a time toward [q], so the
 * snapshot value depends on update order. [Concurrency.Strict] and
 * [Concurrency.HighWrite] lock the body so each step is atomic, but
 * **arrival-order non-determinism remains** — Strict does not reproduce a
 * serial reference. [Concurrency.Relaxed] additionally drops the lock; the
 * single-cell estimate races but stays bounded. The bench reports ~15%
 * absolute error on a `q = 0.5` uniform stream regardless of level.
 */
class FrugalQuantileStat(
    /** Target quantile probability in `[0, 1]`. */
    val q: Double,
    /** Adaptive step size used to chase the quantile. */
    val stepSize: Double = 0.01,
    /** Initial estimate seeding the random walk. */
    val initialEstimate: Double = 0.0,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<QuantileResult> {

    init {
        require(q in 0.0..1.0) { "Quantile q must be between 0.0 and 1.0" }
    }

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val quantile = mode.newDouble(initialEstimate)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        if (weight <= 0.0) return@withLock

        val m = quantile.load()
        val delta = if (value > m) {
            stepSize * q
        } else if (value < m) {
            -stepSize * (1.0 - q)
        } else {
            0.0
        }

        if (delta != 0.0) {
            quantile.add(delta)
        }
    }

    override fun create(concurrency: Concurrency?) = FrugalQuantileStat(
        q,
        stepSize,
        initialEstimate,
        concurrency ?: this.concurrency
    )

    override fun merge(values: QuantileResult) = lock.withLock {
        val current = quantile.load()
        quantile.store((current + values.quantile) / 2.0)
    }

    override fun reset() {
        quantile.store(initialEstimate)
    }

    override fun read(timestampNanos: Long) = QuantileResult(q, quantile.load())
}

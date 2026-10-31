package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode

/**
 * Frugal-streaming single-quantile estimator.
 *
 * Keeps one `Double` of state that drifts toward the target quantile [q]; the drift
 * magnitude is scaled by [stepSize]. Cheap and memory-flat but biased and noisy -
 * use [DDSketchStat] when accuracy matters.
 *
 * Treats observations as unweighted (one unit step per update regardless of
 * weight); scaling the step by raw weight would let a single high-weight
 * observation overshoot the target catastrophically. [merge] averages two
 * estimates as a coarse approximation; frugal sketches do not admit a true
 * associative combine. An estimator that has seen nothing adopts the incoming
 * estimate, and an empty incoming snapshot is skipped, so neither seed is averaged in.
 *
 * **Use cases:** memory-constrained tracking of a single quantile with no
 * accuracy guarantee; IoT-class deployments, debug-only monitoring. For
 * production quantile work prefer [DDSketchStat], [TDigestStat], or
 * [HdrHistogramStat].
 *
 * **Memory:** O(1); one double estimate plus a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Order-dependent random-walk recurrence.
 * [Concurrency.Strict] and [Concurrency.HighWrite] lock each step but do not
 * reproduce a serial reference value; the lock serialises arrival, not
 * order. [Concurrency.Relaxed] additionally drops the lock; the single-cell
 * estimate can drift under contention. No numerical error bound is guaranteed.
 */
class FrugalQuantileStat(
    /** Target quantile probability in `[0, 1]`. */
    val q: Double,
    /** Adaptive step size used to chase the quantile. */
    val stepSize: Double = StatDefaults.FRUGAL_QUANTILE_STEP_SIZE,
    /** Initial estimate seeding the random walk. */
    val initialEstimate: Double = StatDefaults.FRUGAL_QUANTILE_INITIAL_ESTIMATE,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<QuantileResult> {

    init {
        require(q in 0.0..1.0) { "Quantile q must be between 0.0 and 1.0" }
    }

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val quantile = mode.newDouble(initialEstimate)
    private val observed = mode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
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
            observed.store(1L)
        }
    }

    override fun create(concurrency: Concurrency?) = FrugalQuantileStat(
        q,
        stepSize,
        initialEstimate,
        concurrency ?: this.concurrency,
    )

    override fun merge(values: QuantileResult, timestampNanos: Long) = lock.guarded {
        if (!values.hasObservation) return@guarded
        val next = if (observed.load() == 0L) values.quantile else (quantile.load() + values.quantile) / 2.0
        quantile.store(next)
        observed.store(1L)
    }

    override fun reset() = lock.guarded {
        quantile.store(initialEstimate)
        observed.store(0L)
    }

    override fun read(timestampNanos: Long) = QuantileResult(q, quantile.load(), observed.load() != 0L)
}

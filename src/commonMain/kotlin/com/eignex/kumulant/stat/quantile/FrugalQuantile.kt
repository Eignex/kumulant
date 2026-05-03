package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode

/**
 * Frugal-streaming single-quantile estimator.
 *
 * Keeps one `Double` of state that drifts toward the target quantile [q]; the drift
 * magnitude is scaled by [stepSize]. Cheap and memory-flat but biased and noisy —
 * use [DDSketch] when accuracy matters.
 *
 * Treats observations as unweighted (one unit step per update regardless of weight);
 * scaling the step by raw weight would let a single high-weight observation overshoot
 * the target catastrophically. [merge] averages two estimates as a coarse approximation
 * — frugal sketches do not admit a true associative combine.
 */
class FrugalQuantile(
    val q: Double,
    val stepSize: Double = 0.01,
    val initialEstimate: Double = 0.0,
    override val mode: StreamMode = defaultStreamMode,
) : SeriesStat<QuantileResult> {

    init {
        require(q in 0.0..1.0) { "Quantile q must be between 0.0 and 1.0" }
    }

    private val quantile = mode.newDouble(initialEstimate)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return

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

    override fun create(mode: StreamMode?) = FrugalQuantile(
        q,
        stepSize,
        initialEstimate,
        mode ?: this.mode
    )

    override fun merge(values: QuantileResult) {
        val current = quantile.load()
        quantile.store((current + values.quantile) / 2.0)
    }

    override fun reset() {
        quantile.store(initialEstimate)
    }

    override fun read(timestampNanos: Long) = QuantileResult(q, quantile.load())
}

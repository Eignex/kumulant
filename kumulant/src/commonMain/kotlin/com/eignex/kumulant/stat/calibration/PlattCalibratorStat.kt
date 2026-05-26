package com.eignex.kumulant.stat.calibration

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.schema.OptimizerSpec
import com.eignex.kumulant.schema.Sgd
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import com.eignex.kumulant.stat.regression.glm.Link
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionResult
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp

/**
 * Snapshot from [PlattCalibratorStat]: the learned sigmoid parameters and a
 * helper that maps a raw classifier score to a calibrated probability.
 *
 * The calibration map is `1 / (1 + exp(-(slope * x + intercept)))`.
 */
@Serializable
@SerialName("PlattCalibratorResult")
data class PlattCalibratorResult(
    /** Sigmoid slope; positive when higher raw scores correlate with positive labels. */
    val slope: Double,
    /** Sigmoid intercept. */
    val intercept: Double,
    /** Cumulative observation weight folded in. */
    val totalWeights: Double,
) : Result {

    /** Calibrated probability for raw score [x] via `sigmoid(slope * x + intercept)`. */
    fun calibrate(x: Double): Double {
        val eta = slope * x + intercept
        return if (eta >= 0.0) 1.0 / (1.0 + exp(-eta)) else exp(eta) / (1.0 + exp(eta))
    }
}

/**
 * Online Platt scaling: fits a one-feature logistic regression
 * `sigmoid(slope * rawScore + intercept)` over paired
 * `(rawScore, label)` observations where label is in `{0, 1}`. Use to fix the
 * calibration of a classifier whose probability output is poorly aligned with
 * the empirical positive rate.
 *
 * Internally wraps a [StochasticRegressionStat] with `featureSize = 1` and
 * `Link.Logit`. The runtime cost matches a one-dimensional GLM-SGD update per
 * observation.
 *
 * **Use cases:** post-hoc calibration of tree / SVM / naive-Bayes outputs;
 * pair with [ReliabilityStat] to monitor the gap before and after applying
 * the learned mapping.
 *
 * **Memory:** O(1) — backed by [StochasticRegressionStat].
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Inherits [StochasticRegressionStat]'s concurrency model.
 */
class PlattCalibratorStat(
    /** Optimizer driving the underlying logistic regression. */
    val optimizer: OptimizerSpec = Sgd(ConstantRate(1e-2)),
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<PlattCalibratorResult> {

    private val inner = StochasticRegressionStat(
        featureSize = 1,
        optimizer = optimizer,
        link = Link.Logit,
        concurrency = concurrency,
    )

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        inner.update(DenseVector.of(doubleArrayOf(x)), y, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): PlattCalibratorResult {
        val r = inner.read(timestampNanos)
        return PlattCalibratorResult(
            slope = r.weights[0],
            intercept = r.bias,
            totalWeights = r.totalWeights,
        )
    }

    /**
     * Folds another Platt result into this one by reconstructing a
     * [StochasticRegressionStat] snapshot from `(slope, intercept,
     * totalWeights)` and delegating to the underlying merge.
     */
    override fun merge(values: PlattCalibratorResult) {
        val snapshot = StochasticRegressionResult(
            weights = DenseVector.of(doubleArrayOf(values.slope)),
            bias = values.intercept,
            totalWeights = values.totalWeights,
            step = 0L,
            link = Link.Logit,
            sse = 0.0,
        )
        inner.merge(snapshot)
    }

    override fun reset() = inner.reset()

    override fun create(concurrency: Concurrency?) = PlattCalibratorStat(optimizer, concurrency ?: this.concurrency)
}

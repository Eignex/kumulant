package com.eignex.kumulant.core

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.time.Duration
import kotlin.time.DurationUnit

/**
 * Result trait for accumulators that produce a normalised throughput. Implemented
 * by [com.eignex.kumulant.stat.rate.RateResult] (and friends), so downstream code
 * written against [HasRate] works for any rate-shaped stat regardless of the
 * underlying mechanism — uniform-over-window
 * ([com.eignex.kumulant.stat.rate.RateStat]), counter-differentiated
 * ([com.eignex.kumulant.stat.rate.CounterRateStat]), or exponentially-decayed
 * ([com.eignex.kumulant.stat.rate.DecayingRateStat]).
 */
interface HasRate : Result {
    /** Normalised rate in events per second (Hz). Zero when no observations have arrived. */
    val rate: Double

    /**
     * Rescale the throughput to a specific time duration. Equivalent to
     * `rate * duration.toDouble(DurationUnit.SECONDS)`.
     *
     * Example: `rate.per(1.minutes)` returns events per minute.
     */
    fun per(duration: Duration): Double = rate * duration.toDouble(
        DurationUnit.SECONDS,
    )
}

/**
 * Result trait for accumulators that expose a center estimate and a scale
 * estimate. Consumed by the `band` operator (which derives `center ± k * scale`)
 * and by the `Standardize` AST node (which projects `(x - center) / scale`).
 *
 * Implemented by [com.eignex.kumulant.stat.summary.WeightedVarianceResult] (center
 * = mean, scale = stdDev), [com.eignex.kumulant.stat.summary.MomentsResult] (same
 * shape), [com.eignex.kumulant.stat.summary.MadResult] (center = median, scale =
 * MAD), and [com.eignex.kumulant.stat.summary.SummaryResult]. A consumer asking
 * for `HasCenterScale` doesn't need to know which provenance the center/scale
 * came from.
 */
interface HasCenterScale : Result {
    /** Center of the distribution — typically a mean, median, or smoothed level. */
    val center: Double

    /** Scale of the distribution — typically a standard deviation, MAD, or span. */
    val scale: Double
}

/**
 * Result trait for accumulators that expose observed minimum and maximum values.
 * Consumed by the `Low` and `High` AST nodes for min-max scaling and by any
 * downstream that needs the observed range.
 *
 * Implemented by [com.eignex.kumulant.stat.summary.RangeResult] and
 * [com.eignex.kumulant.stat.summary.SummaryResult]. The pair of `(min, max)`
 * extents that come back are over the entire observed stream — windowed views
 * come from wrapping the underlying stat in a `windowed` operator.
 */
interface HasMinMax : Result {
    /** Minimum value observed so far over the accumulator's lifetime (or window). */
    val min: Double

    /** Maximum value observed so far over the accumulator's lifetime (or window). */
    val max: Double
}

/**
 * Result trait for accumulators that expose variance-family quantities. Derived
 * properties [variance] / [stdDev] / [sampleVariance] / [sampleStdDev] all fall
 * out of [sst] (sum of squared deviations) and [totalWeights] without storing
 * redundant fields.
 *
 * Implemented by every Welford-shaped result
 * ([com.eignex.kumulant.stat.summary.WeightedVarianceResult],
 * [com.eignex.kumulant.stat.summary.MomentsResult], the decay-family
 * counterparts) and indirectly by [HasShapeMoments] and [HasRegression].
 *
 * The population vs sample distinction matters when sample size is small. Use
 * [sampleVariance] / [sampleStdDev] when treating the observations as draws
 * from an underlying distribution; use [variance] / [stdDev] when treating
 * the observations as the complete population.
 */
interface HasSampleVariance : Result {
    /** Cumulative weight of observations folded into this result. */
    val totalWeights: Double

    /** Sum of squared deviations from the running mean: `Sum (x - mean)^2 * weight`. */
    val sst: Double get() = variance * totalWeights

    /** Population variance: `[sst] / [totalWeights]`. Zero on an empty stream. */
    val variance: Double get() = if (totalWeights > 0) sst / totalWeights else 0.0

    /** Population standard deviation: `sqrt([variance])`. */
    val stdDev: Double get() = sqrt(variance)

    /** Unbiased sample variance: `[sst] / ([totalWeights] - 1)`. Zero when totalWeights <= 1. */
    val sampleVariance: Double
        get() = if (totalWeights > 1.0) {
            sst / (totalWeights - 1.0)
        } else {
            0.0
        }

    /** Unbiased sample standard deviation: `sqrt([sampleVariance])`. */
    val sampleStdDev: Double get() = sqrt(sampleVariance)
}

/**
 * Result trait for accumulators that expose third and fourth central moments
 * plus skewness and kurtosis. Extends [HasSampleVariance] — every shape moment
 * result is also a variance result.
 *
 * Implemented by [com.eignex.kumulant.stat.summary.MomentsResult]. Use it when
 * the *shape* of the distribution matters — heavy tails (kurtosis), asymmetry
 * (skewness), departures from normality in general.
 *
 * Biased ([skewness], [kurtosis]) and unbiased ([unbiasedSkewness],
 * [unbiasedKurtosis]) variants are both exposed. Pick biased for the moment
 * estimate itself, unbiased when feeding into a small-sample inference test.
 */
interface HasShapeMoments : HasSampleVariance {
    /** Raw 2nd central moment: `Sum (x - mean)^2 * weight`. Identical to [sst]. */
    val m2: Double get() = sst

    /** Raw 3rd central moment: `Sum (x - mean)^3 * weight`. */
    val m3: Double

    /** Raw 4th central moment: `Sum (x - mean)^4 * weight`. */
    val m4: Double

    /**
     * Biased skewness: `(m3 / w) / variance^1.5`. Standardised third moment;
     * 0 for symmetric distributions, positive for right tails, negative for
     * left tails.
     */
    val skewness: Double
        get() {
            val v = variance
            val w = totalWeights
            return if (v > 0 && w > 0) (m3 / w) / v.pow(1.5) else 0.0
        }

    /**
     * Biased excess kurtosis: `(m4 / w) / variance^2 - 3`. Heavy tails are
     * positive; the `- 3` normalises so that a Gaussian has excess kurtosis 0.
     */
    val kurtosis: Double
        get() {
            val v = variance
            val w = totalWeights
            return if (v > 0 && w > 0) (m4 / w) / v.pow(2.0) - 3.0 else 0.0
        }

    /** Sample-size-adjusted (unbiased) skewness. Zero when totalWeights <= 2. */
    val unbiasedSkewness: Double
        get() {
            if (totalWeights <= 2 || skewness == 0.0) return 0.0
            return (sqrt(totalWeights * (totalWeights - 1)) / (totalWeights - 2)) * skewness
        }

    /** Sample-size-adjusted (unbiased) excess kurtosis. Zero when totalWeights <= 3. */
    val unbiasedKurtosis: Double
        get() {
            if (totalWeights <= 3) return 0.0
            val factor1 =
                (totalWeights - 1) / ((totalWeights - 2) * (totalWeights - 3))
            val factor2 = (totalWeights + 1) * kurtosis + 6.0
            return factor1 * factor2
        }
}

/**
 * Result trait for accumulators that produce a fitted linear model
 * `y = bias + weights . x`. Covers both the univariate special case
 * ([HasSlope] — slope + intercept) and the multivariate case
 * ([com.eignex.kumulant.stat.regression.glm.LinearRegressionResult] — `weights`
 * vector + scalar `bias`) behind one surface.
 *
 * Consumers written against [HasLinearModel] handle any linear model
 * uniformly: contextual bandits use it for scoring, downstream pipelines use
 * it for prediction, evaluation metrics use it for residuals.
 *
 * The [predict] method evaluates `bias + Sum_i weights[i] * x[i]`. For
 * generalised linear models with a non-identity link the *linear predictor*
 * `eta` (this method) and the *response mean* `mu = link.invMean(eta)` are
 * distinct; the linear-model-specific result types expose both.
 */
interface HasLinearModel : Result {
    /** Fitted weight per feature, indexed by the same `i` as the input `x[i]`. */
    val weights: VectorView

    /** Fitted bias / intercept term. */
    val bias: Double

    /** Number of features in [weights]. */
    val featureSize: Int get() = weights.size

    /**
     * Evaluate the linear predictor at [x]: `bias + Sum_i weights[i] * x[i]`.
     * For Gaussian regression this is the prediction; for non-identity GLMs
     * this is the linear predictor pre-link.
     */
    fun predict(x: VectorView): Double {
        require(x.size == weights.size) { "x.size=${x.size}, expected ${weights.size}" }
        var sum = bias
        for (i in 0 until weights.size) sum += x[i] * weights[i]
        return sum
    }
}

/**
 * Univariate special case of [HasLinearModel]: `y = slope * x + intercept`.
 * The general weights vector and bias surface are derived from `slope` /
 * `intercept`, so univariate regression results compose with any consumer
 * written against [HasLinearModel] without storing redundant fields.
 *
 * Implemented by [com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult].
 */
interface HasSlope : HasLinearModel {
    /** Fitted slope coefficient `m`. */
    val slope: Double

    /** Fitted intercept `c`. */
    val intercept: Double

    override val weights: VectorView get() = DenseVector.of(doubleArrayOf(slope))
    override val bias: Double get() = intercept
    override val featureSize: Int get() = 1

    /** Evaluate the fitted line at [x]: `slope * x + intercept`. */
    fun predict(x: Double): Double = (slope * x) + intercept
}

/**
 * Result trait for regression error metrics. Extends [HasSampleVariance]
 * because R² is defined as `1 - sse/sst`, and `sst` is the variance-family
 * sum of squared deviations from the mean.
 *
 * Implemented by [com.eignex.kumulant.stat.regression.glm.LinearRegressionResult].
 * Consumers asking for `HasRegression` don't care which underlying regressor
 * produced the snapshot — SGD, Bayesian, diagonal, or hierarchical all
 * surface the same `sse / mse / rmse / rSquared` metrics.
 *
 * Under non-identity GLMs ([Link.Logit][com.eignex.kumulant.stat.regression.glm.Link.Logit],
 * [Link.Log][com.eignex.kumulant.stat.regression.glm.Link.Log]) `sse` carries
 * the per-link deviance, not the classical SSE, and `rSquared` is not the
 * usual Gaussian R². Each GLM result documents its `sse` interpretation.
 */
interface HasRegression : HasSampleVariance {
    /** Sum of squared errors (residuals). Under non-identity GLMs this is the deviance. */
    val sse: Double

    /** Sum of squares due to regression: [sst] - [sse]. */
    val ssr: Double get() = sst - sse

    /** Mean squared error: [sse] / [totalWeights]. Zero on an empty stream. */
    val mse: Double
        get() = if (totalWeights > 0) sse / totalWeights else 0.0

    /** Root mean squared error: `sqrt([mse])`. */
    val rmse: Double
        get() = sqrt(mse)

    /** Coefficient of determination `1 - sse/sst`. Zero when sst is zero. */
    val rSquared: Double
        get() = if (sst > 0) 1.0 - (sse / sst) else 0.0
}

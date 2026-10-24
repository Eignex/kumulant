package com.eignex.kumulant.stat.summary

import com.eignex.koblas.DenseVector
import com.eignex.koblas.Vector
import com.eignex.koblas.dot
import com.eignex.koblas.scale
import com.eignex.koblas.sum
import kotlin.math.sqrt

/** Weighted mean with separate response dispersion and uncertainty in its estimate. */
data class MeanEstimate(
    /** Weighted response mean. */
    val mean: Double,
    /** Kish effective sample size; invariant under a common rescaling of weights. */
    val effectiveSamples: Double,
    /** Unbiased weighted response dispersion, or zero when it cannot be estimated. */
    val responseVariance: Double,
    /** Weighted average supplied observation variance, null when none is supplied. */
    val knownNoiseVariance: Double?,
    /** Maximum of empirical sampling variance and propagated known noise; null without enough evidence. */
    val varianceOfMean: Double?,
) {
    /** Standard error, infinite when a single unknown-noise observation cannot estimate it. */
    val standardError: Double get() = varianceOfMean?.let(::sqrt) ?: Double.POSITIVE_INFINITY

    /** Descriptive mean minus a standard-error multiple; not a sequential confidence bound. */
    fun lowerBound(standardErrors: Double): Double {
        require(standardErrors >= 0.0 && standardErrors.isFinite())
        return if (standardErrors == 0.0) mean else mean - standardErrors * standardError
    }

    /** Descriptive mean plus a standard-error multiple; not a sequential confidence bound. */
    fun upperBound(standardErrors: Double): Double {
        require(standardErrors >= 0.0 && standardErrors.isFinite())
        return if (standardErrors == 0.0) mean else mean + standardErrors * standardError
    }
}

/**
 * Estimate a weighted mean using independent observation units and optional known noise variances.
 * Weights express relative importance, not replicate counts. Variances may be NaN where unknown.
 * Empirical mean variance uses Kish sample size and the unbiased weighted dispersion; supplied
 * noise propagates as sum of squared normalized weights times variance. The larger estimate is
 * retained rather than counting noise twice. Unknown noise at a singleton remains unestimated.
 * Partial supplied noise gives only its known contribution. This is a descriptive approximation,
 * not a calibrated posterior or an anytime-valid confidence interval.
 */
fun estimateMean(values: Vector, weights: Vector? = null, noiseVariances: Vector? = null): MeanEstimate {
    require(values.size > 0 && (weights == null || weights.size == values.size))
    require(noiseVariances == null || noiseVariances.size == values.size)
    require(values.toDoubleArray().all { it.isFinite() })
    val normalized = DenseVector.of(weights?.toDoubleArray() ?: DoubleArray(values.size) { 1.0 })
    require(normalized.toDoubleArray().all { it >= 0.0 && it.isFinite() })
    val maximum = normalized.toDoubleArray().max()
    require(maximum > 0.0)
    for (i in 0 until normalized.size) normalized[i] = normalized[i] / maximum
    normalized.scale(1.0 / normalized.sum())
    val squareSum = normalized dot normalized
    val moments = VarianceStat()
    for (i in 0 until values.size) moments.update(values[i], 0L, normalized[i])
    val summary = moments.read(0L)
    val dispersion = if (squareSum < 1.0) summary.variance / (1.0 - squareSum) else 0.0
    var noiseWeight = 0.0
    var averageNoise = 0.0
    var propagatedNoise = 0.0
    if (noiseVariances != null) {
        for (i in 0 until values.size) {
            val noise = noiseVariances[i]
            require(noise.isNaN() || (noise >= 0.0 && noise.isFinite()))
            if (noise.isNaN() || normalized[i] == 0.0) continue
            noiseWeight += normalized[i]
            averageNoise += normalized[i] * noise
            propagatedNoise += normalized[i] * normalized[i] * noise
        }
    }
    val empirical = if (squareSum < 1.0) dispersion * squareSum else null
    return MeanEstimate(
        summary.mean,
        1.0 / squareSum,
        dispersion,
        if (noiseWeight > 0.0) averageNoise / noiseWeight else null,
        if (empirical != null || noiseWeight > 0.0) maxOf(empirical ?: 0.0, propagatedNoise) else null,
    )
}

/** Estimate paired left-minus-right differences without assuming independent responses within a pair. */
fun pairedMeanDifference(left: Vector, right: Vector): MeanEstimate {
    require(left.size == right.size)
    return estimateMean(DenseVector.of(DoubleArray(left.size) { left[it] - right[it] }))
}

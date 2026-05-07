package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.sqrt

/**
 * Streaming mean CRPS for Gaussian forecasts. Each update is a 3-vector
 * `[mean, stdDev, y]`; the per-row score uses the closed-form Gaussian CRPS
 * (Gneiting & Raftery 2007, eq. 5), aggregated as a [Mean].
 */
class CrpsGaussian(
    override val concurrency: Concurrency = Concurrency.None,
) : VectorStat<WeightedMeanResult> {

    private val inner = Mean(concurrency)

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        require(vector.size == 3) {
            "CrpsGaussian expects [mean, stdDev, y]; got size ${vector.size}"
        }
        val crps = gaussianCrps(mean = vector[0], stdDev = vector[1], y = vector[2])
        inner.update(crps, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): WeightedMeanResult = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?): CrpsGaussian =
        CrpsGaussian(concurrency ?: this.concurrency)
}

/**
 * Closed-form CRPS for `N(mean, stdDev²)` against observation [y]:
 * `σ · [z·(2Φ(z) − 1) + 2φ(z) − 1/√π]` where `z = (y − μ)/σ`.
 */
private fun gaussianCrps(mean: Double, stdDev: Double, y: Double): Double {
    require(stdDev >= 0.0) { "stdDev must be non-negative; got $stdDev" }
    if (stdDev == 0.0) return abs(y - mean)
    val z = (y - mean) / stdDev
    return stdDev * (z * (2.0 * stdNormalCdf(z) - 1.0) + 2.0 * stdNormalPdf(z) - 1.0 / sqrt(PI))
}

private fun stdNormalPdf(z: Double): Double = exp(-0.5 * z * z) / sqrt(2.0 * PI)

private fun stdNormalCdf(z: Double): Double = 0.5 * (1.0 + erf(z / SQRT2))

private const val SQRT2: Double = 1.4142135623730951

/** Abramowitz & Stegun 7.1.26 erf approximation; max error ~1.5e-7 — sufficient for CRPS. */
private fun erf(x: Double): Double {
    val sign = if (x < 0.0) -1.0 else 1.0
    val ax = abs(x)
    val t = 1.0 / (1.0 + 0.3275911 * ax)
    val y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * exp(-ax * ax)
    return sign * y
}

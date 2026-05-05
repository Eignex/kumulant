package com.eignex.kumulant.forecast

import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.sqrt

/** Standard normal PDF: `(1/√(2π))·exp(-z²/2)`. */
internal fun stdNormalPdf(z: Double): Double = exp(-0.5 * z * z) / sqrt(2.0 * PI)

/**
 * Standard normal CDF using Abramowitz & Stegun 7.1.26 erf approximation.
 * Max absolute error ~1.5e-7 — sufficient for CRPS evaluation.
 */
internal fun stdNormalCdf(z: Double): Double = 0.5 * (1.0 + erf(z / SQRT2))

private const val SQRT2: Double = 1.4142135623730951

/** Abramowitz & Stegun 7.1.26 erf approximation; max error ~1.5e-7. */
private fun erf(x: Double): Double {
    val sign = if (x < 0.0) -1.0 else 1.0
    val ax = abs(x)
    val t = 1.0 / (1.0 + 0.3275911 * ax)
    val y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * exp(-ax * ax)
    return sign * y
}

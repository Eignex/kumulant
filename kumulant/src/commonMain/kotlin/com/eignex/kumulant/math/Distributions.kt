package com.eignex.kumulant.math

import kotlin.random.Random
import com.eignex.util.random.nextBeta as utilNextBeta
import com.eignex.util.random.nextGamma as utilNextGamma
import com.eignex.util.random.nextNormal as utilNextNormal
import com.eignex.util.random.nextPoissonOne as utilNextPoissonOne

/** Draw from `N(mean, std^2)` using util's Ziggurat sampler. Stateless beyond the [Random] receiver. */
fun Random.nextNormal(mean: Double = 0.0, std: Double = 1.0): Double = utilNextNormal(mean, std)

/**
 * Draw from `Gamma(alpha, 1)` (unit rate). Marsaglia-Tsang (2000) for `alpha >= 1`
 * with Stuart's power-of-uniform boost for `alpha < 1`. Two fast paths for common
 * parameter values:
 *  - `alpha == 1.0`: returns `-ln(U)` directly (Exponential). Bypasses the
 *    Gaussian rejection loop entirely.
 *  - `alpha` a small positive integer (2..5): sums `alpha` Exponential samples.
 *    Cheaper than one Gaussian + acceptance test at that scale, and the
 *    integer check pays for itself in the common Poisson-prior usage pattern.
 */
fun Random.nextGamma(alpha: Double): Double = utilNextGamma(alpha)

/**
 * Draw from `Beta(alpha, beta)` via the two-gamma quotient `X / (X + Y)` where
 * `X ~ Gamma(alpha)`, `Y ~ Gamma(beta)`. Fast paths for the trivial special cases:
 *  - `alpha == 1.0 && beta == 1.0`: returns `nextDouble()` (Uniform).
 *  - `alpha == 1.0`: returns `1 - U^(1/beta)` (power distribution).
 *  - `beta == 1.0`: returns `U^(1/alpha)`.
 */
fun Random.nextBeta(alpha: Double, beta: Double): Double = utilNextBeta(alpha, beta)

/** Knuth's Poisson sampler at lambda=1; returns 0/1/2/... with mass `e^{-1} / k!`. */
fun Random.nextPoissonOne(): Int = utilNextPoissonOne()

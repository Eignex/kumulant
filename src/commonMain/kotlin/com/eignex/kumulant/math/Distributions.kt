package com.eignex.kumulant.math

import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Random-variate generators for the distributions kumulant's bandit/regression code
 * needs. Implementations are textbook-standard: Marsaglia polar for Gaussian,
 * Marsaglia-Tsang for Gamma, two-gamma quotient for Beta. Each function lives as an
 * extension on [Random] for one-off use, with hot-path callers preferring
 * [GaussianSampler] (which caches the polar method's spare).
 */

private const val MIN_POS = Double.MIN_VALUE

/** Skip exact zero so callers can take `ln(u)` without checking. */
private fun Random.nextDoublePos(): Double {
    while (true) {
        val u = nextDouble()
        if (u > 0.0) return u
    }
}

// === Normal ================================================================

fun Random.nextNormal(mean: Float, std: Float): Float = nextNormal(mean.toDouble(), std.toDouble()).toFloat()

/**
 * Draw from `N(mean, std²)` via Marsaglia polar. Each accepted (u, v) pair yields
 * two independent Gaussians; the bare extension discards the second. For repeated
 * draws against the same [Random], construct a [GaussianSampler] and call `next()` —
 * the cached spare halves throughput on Gaussian-bound loops.
 */
fun Random.nextNormal(mean: Double = 0.0, std: Double = 1.0): Double {
    var u: Double
    var s: Double
    do {
        u = nextDouble() * 2 - 1
        val v = nextDouble() * 2 - 1
        s = u * u + v * v
    } while (s >= 1.0 || s == 0.0)
    val mul = sqrt(-2.0 * ln(s) / s)
    return mean + std * u * mul
}

/**
 * Cached-spare Marsaglia polar sampler. Each accepted (u, v) pair yields two
 * standard Gaussians; we return one and stash the other for the next call. For
 * loops that draw N Gaussians per round (e.g.
 * [com.eignex.kumulant.stat.regression.MultivariateGaussian] sampling N weight
 * components, [com.eignex.kumulant.stat.regression.FactorisedGaussian] sampling
 * per-coordinate noise) this halves the wall-clock cost of the rejection loop.
 *
 * Not thread-safe — one sampler per drawing thread.
 */
class GaussianSampler(private val rng: Random) {
    // NaN sentinel = no spare cached. Spare is a *standard* Gaussian (mean=0, std=1);
    // [next] applies the per-call (mean, std) scaling on top.
    private var spare: Double = Double.NaN

    /** Draw from `N(mean, std²)`. */
    fun next(mean: Double = 0.0, std: Double = 1.0): Double {
        val cached = spare
        if (!cached.isNaN()) {
            spare = Double.NaN
            return mean + std * cached
        }
        var u: Double
        var v: Double
        var s: Double
        do {
            u = rng.nextDouble() * 2 - 1
            v = rng.nextDouble() * 2 - 1
            s = u * u + v * v
        } while (s >= 1.0 || s == 0.0)
        val mul = sqrt(-2.0 * ln(s) / s)
        spare = v * mul
        return mean + std * (u * mul)
    }
}

// === Log-Normal ============================================================

/**
 * Draw from a log-normal distribution parameterised by **real-scale** [mean] and
 * [variance] (not the underlying Normal's mu/sigma). Used by log-normal posteriors
 * where the bandit observes positive-valued rewards and wants a multiplicative
 * noise model.
 */
fun Random.nextLogNormal(mean: Double, variance: Double): Double {
    require(mean > 0.0) { "nextLogNormal requires mean > 0; got $mean" }
    require(variance >= 0.0) { "nextLogNormal requires variance >= 0; got $variance" }
    val phi = sqrt(variance + mean * mean)
    val mu = ln(mean * mean / phi)
    val sigma = sqrt(ln(phi * phi / (mean * mean)))
    return exp(nextNormal(mu, sigma))
}

// === Gamma =================================================================

/**
 * Draw from `Gamma(alpha, 1)` (unit rate). Marsaglia-Tsang (2000) for `alpha ≥ 1`
 * with Stuart's power-of-uniform boost for `alpha < 1`. Two fast paths for common
 * parameter values:
 *  - `alpha == 1.0`: returns `-ln(U)` directly (Exponential). Bypasses the
 *    Gaussian rejection loop entirely.
 *  - `alpha` a small positive integer (2..5): sums `α` Exponential samples.
 *    Cheaper than one Gaussian + acceptance test at that scale, and the
 *    integer check pays for itself in the common Poisson-prior usage pattern.
 */
fun Random.nextGamma(alpha: Double): Double {
    require(alpha > 0.0) { "nextGamma requires alpha > 0; got $alpha" }
    if (alpha == 1.0) return -ln(nextDoublePos())
    if (alpha <= 5.0) {
        val k = alpha.toInt()
        if (k.toDouble() == alpha) {
            var s = 0.0
            repeat(k) { s -= ln(nextDoublePos()) }
            return s
        }
    }
    if (alpha < 1.0) {
        val u = nextDoublePos()
        val g = nextGamma(1.0 + alpha) * u.pow(1.0 / alpha)
        return if (g == 0.0) MIN_POS else g
    }
    val d = alpha - 1.0 / 3.0
    val c = (1.0 / 3.0) / sqrt(d)
    while (true) {
        var v: Double
        var x: Double
        do {
            x = nextNormal(0.0, 1.0)
            v = 1.0 + c * x
        } while (v <= 0)
        v = v * v * v
        val u = nextDoublePos()
        if (u < 1 - 0.0331 * x * x * x * x) return d * v
        if (ln(u) < 0.5 * x * x + d * (1 - v + ln(v))) return d * v
    }
}

// === Beta ==================================================================

/**
 * Draw from `Beta(alpha, beta)` via the two-gamma quotient `X / (X + Y)` where
 * `X ~ Gamma(alpha)`, `Y ~ Gamma(beta)`. Fast paths for the trivial special cases:
 *  - `alpha == 1.0 && beta == 1.0`: returns `nextDouble()` (Uniform).
 *  - `alpha == 1.0`: returns `1 - U^(1/beta)` (power distribution).
 *  - `beta == 1.0`: returns `U^(1/alpha)`.
 */
fun Random.nextBeta(alpha: Double, beta: Double): Double {
    require(alpha > 0.0) { "nextBeta requires alpha > 0; got $alpha" }
    require(beta > 0.0) { "nextBeta requires beta > 0; got $beta" }
    if (alpha == 1.0 && beta == 1.0) return nextDouble()
    if (alpha == 1.0) return 1.0 - nextDoublePos().pow(1.0 / beta)
    if (beta == 1.0) return nextDoublePos().pow(1.0 / alpha)
    val a = nextGamma(alpha)
    val b = nextGamma(beta)
    val s = a + b
    return if (s > 0.0) a / s else if (a > 0.0) 1.0 else 0.0
}

package com.eignex.kumulant.math

import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.ln1p
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Random-variate generators for the distributions kumulant's bandit and regression
 * code needs. Implementations are textbook-standard: Ziggurat for Gaussian,
 * Marsaglia-Tsang for Gamma, two-gamma quotient for Beta.
 */

private const val MIN_POS = Double.MIN_VALUE

/** Skip exact zero so callers can take `ln(u)` without checking. */
private fun Random.nextDoublePos(): Double {
    while (true) {
        val u = nextDouble()
        if (u > 0.0) return u
    }
}

/** Float overload of [nextNormal]; widens to Double, samples, narrows back. */
fun Random.nextNormal(mean: Float, std: Float): Float = nextNormal(mean.toDouble(), std.toDouble()).toFloat()

/**
 * Draw from `N(mean, std^2)` via Marsaglia & Tsang's Ziggurat algorithm. The fast
 * path is one `nextInt()` + table lookup + comparison; ~97% of draws complete there.
 * The slow path handles the tail beyond `R = 3.4426` and the "wedge" regions
 * outside each layer's inner rectangle.
 *
 * Stateless beyond the [Random] receiver. The precomputed layer tables
 * ([ZIGGURAT_KN], [ZIGGURAT_WN], [ZIGGURAT_FN]) initialise once at class load, so
 * call sites that need many Gaussians can loop on the extension directly.
 *
 * Reference: Marsaglia, G. & Tsang, W. W. (2000), "The Ziggurat Method for
 * Generating Random Variables", Journal of Statistical Software, 5(8).
 */
fun Random.nextNormal(mean: Double = 0.0, std: Double = 1.0): Double = mean + std * standardNormal()

private fun Random.standardNormal(): Double {
    while (true) {
        val hz = nextInt()
        val iz = hz and (ZIGGURAT_N - 1)
        // -Int.MIN_VALUE is still Int.MIN_VALUE, so a plain `-hz` would leave absHz negative once every
        // 2^32 draws, making the comparison below trivially true and taking the layer-0 inner-rectangle
        // fast path where the tail sampler is required. Clamping that one input to Int.MAX_VALUE avoids
        // widening to Long: every ZIGGURAT_KN entry is at most Int.MAX_VALUE, so both Int.MAX_VALUE and
        // the true magnitude 2^31 fail the test identically and fall through to the tail. Widening is
        // correct too, but Long arithmetic can require boxed allocations and a
        // compare() call in the 97% fast path of every normal draw.
        val absHz = if (hz >= 0) {
            hz
        } else if (hz == Int.MIN_VALUE) {
            Int.MAX_VALUE
        } else {
            -hz
        }
        // Fast path: ~97% of draws land in the inner rectangle of their layer.
        if (absHz < ZIGGURAT_KN[iz]) return hz * ZIGGURAT_WN[iz]
        // Slow path: tail beyond R (iz == 0) or wedge test.
        if (iz == 0) {
            var x: Double
            var y: Double
            do {
                x = -ln(nextDouble().coerceAtLeast(MIN_POS)) * ZIGGURAT_R_INV
                y = -ln(nextDouble().coerceAtLeast(MIN_POS))
            } while (y + y < x * x)
            return if (hz > 0) ZIGGURAT_R + x else -(ZIGGURAT_R + x)
        }
        val x = hz * ZIGGURAT_WN[iz]
        if (ZIGGURAT_FN[iz] + nextDouble() * (ZIGGURAT_FN[iz - 1] - ZIGGURAT_FN[iz]) < exp(-0.5 * x * x)) {
            return x
        }
    }
}

// Marsaglia 2000 constants for N=128 layers. Computed once at class load.
private const val ZIGGURAT_N = 128
private const val ZIGGURAT_R = 3.442619855899
private const val ZIGGURAT_R_INV = 1.0 / ZIGGURAT_R
private const val ZIGGURAT_V = 9.91256303526217e-3
private const val ZIGGURAT_M1 = 2147483648.0 // 2^31 as Double

private val ZIGGURAT_KN = IntArray(ZIGGURAT_N)
private val ZIGGURAT_WN = DoubleArray(ZIGGURAT_N)
private val ZIGGURAT_FN = DoubleArray(ZIGGURAT_N)

@Suppress("UnusedPrivateProperty")
private val ZIGGURAT_INIT = run {
    var dn = ZIGGURAT_R
    var tn = dn
    val q = ZIGGURAT_V / exp(-0.5 * ZIGGURAT_R * ZIGGURAT_R)
    ZIGGURAT_KN[0] = ((ZIGGURAT_R / q) * ZIGGURAT_M1).toInt()
    ZIGGURAT_KN[1] = 0
    ZIGGURAT_WN[0] = q / ZIGGURAT_M1
    ZIGGURAT_WN[ZIGGURAT_N - 1] = ZIGGURAT_R / ZIGGURAT_M1
    ZIGGURAT_FN[0] = 1.0
    ZIGGURAT_FN[ZIGGURAT_N - 1] = exp(-0.5 * ZIGGURAT_R * ZIGGURAT_R)
    for (i in ZIGGURAT_N - 2 downTo 1) {
        dn = sqrt(-2.0 * ln(ZIGGURAT_V / dn + exp(-0.5 * dn * dn)))
        ZIGGURAT_KN[i + 1] = ((dn / tn) * ZIGGURAT_M1).toInt()
        tn = dn
        ZIGGURAT_WN[i] = dn / ZIGGURAT_M1
        ZIGGURAT_FN[i] = exp(-0.5 * dn * dn)
    }
    Unit
}

/**
 * Draw from a log-normal distribution parameterised by real-scale [mean] and
 * [variance] (not the underlying Normal's mu/sigma). Used by log-normal posteriors
 * where the bandit observes positive-valued rewards under a multiplicative noise
 * model.
 */
fun Random.nextLogNormal(mean: Double, variance: Double): Double {
    require(mean > 0.0) { "nextLogNormal requires mean > 0; got $mean" }
    require(variance >= 0.0) { "nextLogNormal requires variance >= 0; got $variance" }
    // Derive sigma from the coefficient of variation rather than from phi/mean directly: squaring the
    // mean underflows to 0.0 well inside the documented domain (mean > 0), which makes
    // ln(phi^2 / mean^2) evaluate 0.0/0.0 and return NaN, and one exponent earlier yields a
    // non-positive draw from a strictly positive distribution. variance / mean^2 is the same ratio
    // with one fewer chance to underflow, and log1p keeps precision when it is small.
    val cv2 = (variance / mean) / mean
    val sigma = sqrt(ln1p(cv2))
    val mu = ln(mean) - 0.5 * sigma * sigma
    return exp(nextNormal(mu, sigma))
}

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
    // nextGamma is non-negative on every path, so `s <= 0.0` means both draws were zero. A NaN draw
    // lands here too, since comparisons against NaN are false, and zero is the right answer for both.
    return if (s > 0.0) a / s else 0.0
}

/** Knuth's Poisson sampler at lambda=1; returns 0/1/2/... with mass `e^{-1} / k!`. */
fun Random.nextPoissonOne(): Int {
    val l = exp(-1.0)
    var k = 0
    var p = 1.0
    do {
        k++
        p *= nextDouble()
    } while (p > l)
    return k - 1
}

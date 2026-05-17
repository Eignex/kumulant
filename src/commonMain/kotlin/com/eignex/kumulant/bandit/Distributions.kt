package com.eignex.kumulant.bandit

import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

private const val MIN_POS = Double.MIN_VALUE

fun Random.nextNormal(mean: Float, std: Float): Float = nextNormal(mean.toDouble(), std.toDouble()).toFloat()

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
 * Draw from a log-normal distribution parameterised by **real-scale** [mean] and
 * [variance] (not the underlying Normal's mu/sigma). Used by log-normal posteriors
 * where the bandit observes positive-valued rewards and wants a multiplicative
 * noise model.
 */
fun Random.nextLogNormal(mean: Double, variance: Double): Double {
    val phi = sqrt(variance + mean * mean)
    val mu = ln(mean * mean / phi)
    val sigma = sqrt(ln(phi * phi / (mean * mean)))
    return exp(nextNormal(mu, sigma))
}

private fun Random.nextDoublePos(): Double {
    while (true) {
        val u = nextDouble()
        if (u > 0.0) return u
    }
}

fun Random.nextGamma(alpha: Double): Double {
    require(alpha > 0)
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

fun Random.nextBeta(alpha: Double, beta: Double): Double {
    val a = nextGamma(alpha)
    val b = nextGamma(beta)
    val s = a + b
    val denom = if (s == max(a, b)) s + MIN_POS else s
    return a / denom
}

package com.eignex.kumulant.forecast

import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.sqrt

/**
 * A probabilistic forecast that can be scored against a single observation via
 * the Continuous Ranked Probability Score (CRPS).
 *
 * Sampling from the forecast is intentionally absent: kumulant collects the
 * statistics needed for downstream sampling, but the act of drawing samples
 * (e.g. Thompson sampling in a bandit) lives in the consuming library.
 */
sealed interface Forecast {
    /**
     * Continuous Ranked Probability Score against [y]. Lower is better; equals
     * `|forecast - y|` when the forecast is a point mass.
     */
    fun crps(y: Double): Double
}

/**
 * Forecast distribution `N(mean, stdDev²)`. Closed-form CRPS via
 * `σ · [z·(2Φ(z) − 1) + 2φ(z) − 1/√π]` where `z = (y − μ)/σ`
 * (Gneiting & Raftery 2007, eq. 5).
 */
data class GaussianForecast(val mean: Double, val stdDev: Double) : Forecast {
    init { require(stdDev >= 0.0) { "stdDev must be non-negative; got $stdDev" } }

    override fun crps(y: Double): Double {
        if (stdDev == 0.0) return abs(y - mean)
        val z = (y - mean) / stdDev
        return stdDev * (z * (2.0 * stdNormalCdf(z) - 1.0) + 2.0 * stdNormalPdf(z) - 1.0 / sqrt(PI))
    }
}

/**
 * Empirical forecast represented by an ensemble of [samples]. CRPS uses the
 * standard estimator `(1/m)·Σ|xᵢ − y| − 1/(2m²)·Σᵢⱼ|xᵢ − xⱼ|`. The pairwise
 * second term is computed in `O(m log m)` via the sorted-rank identity
 * `Σᵢⱼ |xᵢ − xⱼ| = 2·Σₖ k·(m − k)·(x_(k+1) − x_(k))`.
 *
 * The samples array is copied internally on first use so caller-side mutation
 * after construction does not affect scoring.
 */
class EnsembleForecast(samples: DoubleArray) : Forecast {
    private val sorted: DoubleArray = samples.copyOf().also { it.sort() }

    /** Defensive copy of the original samples (in original order). */
    val samples: DoubleArray get() = sorted.copyOf()

    /** Number of samples in the ensemble. */
    val size: Int get() = sorted.size

    override fun crps(y: Double): Double {
        val m = sorted.size
        if (m == 0) return Double.NaN
        var meanAbs = 0.0
        for (x in sorted) meanAbs += abs(x - y)
        meanAbs /= m
        var pairSum = 0.0
        for (k in 0 until m - 1) {
            pairSum += (k + 1).toLong() * (m - k - 1).toLong() * (sorted[k + 1] - sorted[k])
        }
        val pairTerm = pairSum / (m.toLong() * m)
        return meanAbs - pairTerm
    }

    override fun equals(other: Any?): Boolean = other is EnsembleForecast && sorted.contentEquals(other.sorted)
    override fun hashCode(): Int = sorted.contentHashCode()
    override fun toString(): String = "EnsembleForecast(size=$size)"
}

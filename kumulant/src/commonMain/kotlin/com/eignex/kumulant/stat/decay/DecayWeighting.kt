package com.eignex.kumulant.stat.decay

import kotlin.math.exp
import kotlin.math.ln
import kotlin.time.Duration

/**
 * Shared decay strategy for exponentially weighted stats.
 *
 * Two clocks: [HalfLife] drives decay by wall-clock elapsed time; [Alpha] drives decay
 * by cumulative observation weight. Both parameterise the family with [alpha], the
 * rate parameter used in `exp(-alpha*progress)` - where *progress* is either nanoseconds
 * elapsed or weight accumulated depending on the clock.
 */
sealed interface DecayWeighting {
    /** Decay rate; larger alpha means faster decay. */
    val alpha: Double

    /**
     * Bias-correction factor `1 - exp(-alpha*w)`, the fraction of a running exponentially
     * weighted mean's bias that has been "worked off" after observing cumulative weight [w].
     * Returns 0 for w = 0 to avoid 0/0 when no observations have arrived.
     *
     * For a small `alpha*w` the closed form loses the whole value to rounding: `exp(-x)` is
     * `1.0` for any `x` below the double epsilon, so `1 - exp(-x)` is exactly `0.0` while the
     * true correction is `x`. Callers divide by this, so a spurious zero reads as either NaN or
     * a silent zero depending on which stat asks. Fall back to the first-order term, which is
     * accurate to relative `x/2` precisely where the closed form has none left.
     */
    fun correction(w: Double): Double {
        if (w == 0.0) return 0.0
        val exact = 1.0 - exp(-alpha * w)
        // Only substitute when the closed form has collapsed to exactly zero. A negative `w` is a
        // downdate and makes `exact` legitimately negative, which must be kept as it is.
        return if (exact != 0.0) exact else alpha * w
    }

    /** Time-driven decay: alpha = ln(2) / halfLife, progress measured in nanoseconds. */
    class HalfLife(
        /** Wall-clock half-life of past contributions. */
        val halfLife: Duration,
    ) : DecayWeighting {
        init {
            // inWholeNanoseconds truncates, so anything under 1ns (Duration.ZERO included) would
            // divide by zero and make alpha infinite, which poisons every later exp() with NaN.
            require(halfLife.inWholeNanoseconds > 0L) {
                "halfLife must be at least 1ns, got $halfLife"
            }
        }

        override val alpha: Double = ln(2.0) / halfLife.inWholeNanoseconds.toDouble()
    }

    /** Weight-driven decay: alpha given explicitly, progress measured in cumulative weight. */
    class Alpha(override val alpha: Double) : DecayWeighting {
        init {
            // A negative alpha is not a slow decay but exponential growth, and NaN poisons every
            // later term. Zero is allowed and means "never move": HoltStat passes `beta = 0.0` to
            // disable trend smoothing, where the correction is a multiplier rather than a divisor.
            require(alpha >= 0.0 && alpha.isFinite()) {
                "alpha must be non-negative and finite, got $alpha"
            }
        }
    }
}

/** Shorthand for [DecayWeighting.HalfLife] - usable as a shared weighting across stats. */
fun halfLife(halfLife: Duration): DecayWeighting.HalfLife = DecayWeighting.HalfLife(halfLife)

/** Shorthand for [DecayWeighting.Alpha] - usable as a shared weighting across stats. */
fun alpha(alpha: Double): DecayWeighting.Alpha = DecayWeighting.Alpha(alpha)

/**
 * Divide a biased accumulator by its [DecayWeighting.correction], reporting an unmoved accumulator as
 * zero.
 *
 * Three getters across [EwmaMeanStat] and [EwmaVarianceStat] spelled this out. The zero branch is
 * reachable only for `alpha == 0.0` - no smoothing at all - where the accumulator has never left zero and
 * the ratio is 0/0; reporting the accumulator beats reporting NaN. `EwmaMeanStat` records that the two
 * stats "used to disagree here", and the fix was applied by copying rather than by extracting, which
 * left the mean version carrying an extra `w == 0.0` guard the variance version does without. That guard
 * was redundant either way, since `correction(0.0)` is already `0.0`.
 *
 * An extension rather than an interface member: [DecayWeighting] is public API, and this is a convenience
 * for three internal call sites, not something the sealed hierarchy should publish.
 */
internal fun DecayWeighting.debias(biased: Double, totalWeight: Double): Double {
    val correction = correction(totalWeight)
    return if (correction == 0.0) 0.0 else biased / correction
}

package com.eignex.kumulant.concurrent

import kotlin.math.exp
import kotlin.math.ln
import kotlin.time.Duration

/**
 * Shared decay strategy for exponentially weighted stats.
 *
 * Two clocks: [HalfLife] drives decay by wall-clock elapsed time; [Alpha] drives decay
 * by cumulative observation weight. Both parameterise the family with [alpha], the
 * rate parameter used in `exp(-α·progress)` — where *progress* is either nanoseconds
 * elapsed or weight accumulated depending on the clock.
 */
sealed interface DecayWeighting {
    /** Decay rate; larger α means faster decay. */
    val alpha: Double

    /**
     * Bias-correction factor `1 − exp(−α·w)`, the fraction of a running exponentially
     * weighted mean's bias that has been "worked off" after observing cumulative weight [w].
     * Returns 0 for w = 0 to avoid 0/0 when no observations have arrived.
     */
    fun correction(w: Double): Double =
        if (w == 0.0) 0.0 else 1.0 - exp(-alpha * w)

    /** Time-driven decay: α = ln(2) / halfLife, progress measured in nanoseconds. */
    class HalfLife(val halfLife: Duration) : DecayWeighting {
        override val alpha: Double = ln(2.0) / halfLife.inWholeNanoseconds.toDouble()
    }

    /** Weight-driven decay: α given explicitly, progress measured in cumulative weight. */
    class Alpha(override val alpha: Double) : DecayWeighting
}

/** Shorthand for [DecayWeighting.HalfLife] — usable as a shared weighting across stats. */
fun halfLife(halfLife: Duration): DecayWeighting.HalfLife = DecayWeighting.HalfLife(halfLife)

/** Shorthand for [DecayWeighting.Alpha] — usable as a shared weighting across stats. */
fun alpha(alpha: Double): DecayWeighting.Alpha = DecayWeighting.Alpha(alpha)

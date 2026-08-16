package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.pow

/**
 * Snapshot from a [HoltStat]: the current level and trend plus the damping factor.
 *
 * Call [forecast] to project `steps` updates into the future under the same trend.
 */
@Serializable
@SerialName("HoltResult")
data class HoltResult(
    /** Smoothed level (one-step-ahead estimate at the current moment). */
    val level: Double,
    /** Smoothed per-update trend. */
    val trend: Double,
    /** Damping factor applied to the trend each step (`1.0` for un-damped Holt). */
    val phi: Double,
) : Result {
    /**
     * Project `steps` updates ahead. With damping `phi < 1.0` this matches the standard
     * damped-trend forecast `level + (phi + phi^2 + ... + phi^steps) * trend`; with
     * `phi == 1.0` it collapses to `level + steps * trend`.
     */
    fun forecast(steps: Int): Double {
        require(steps >= 0) { "forecast steps must be >= 0, got $steps" }
        if (steps == 0) return level
        if (phi == 1.0) return level + steps.toDouble() * trend
        // Closed form of the geometric series, as the comment always claimed: looping `steps` times
        // made forecast(Int.MAX_VALUE) stall for seconds for no gain in accuracy.
        val sum = phi * (1.0 - phi.pow(steps.toDouble())) / (1.0 - phi)
        return level + sum * trend
    }
}

/**
 * Double exponential smoothing (Holt's method) with optional damping.
 *
 * Per-update recurrence (treating `weight` as the smoothing speed multiplier in the
 * same shape as [com.eignex.kumulant.stat.decay.EwmaMeanStat]):
 *
 * ```
 * a   = 1 - exp(-alpha * weight)
 * b   = 1 - exp(-beta  * weight)
 * prev = level
 * level = a * value + (1 - a) * (prev + phi * trend)
 * trend = b * (level - prev) + (1 - b) * phi * trend
 * ```
 *
 * The first update seeds `level = value` and `trend = 0`. With `phi == 1.0` this is
 * standard Holt smoothing; with `phi < 1.0` the trend is geometrically damped.
 *
 * **Use cases:** smoothing a noisy series and producing short-horizon forecasts when a
 * non-zero local trend is expected. [com.eignex.kumulant.stat.decay.EwmaMeanStat] is the special case `beta == 0`.
 *
 * **Memory:** O(1); two doubles plus a lock.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Order-dependent recurrence, same model as [com.eignex.kumulant.stat.decay.EwmaMeanStat]. [Concurrency.Strict]
 * and [Concurrency.HighWrite] lock the body so each update is atomic but the lock serialises
 * arrival, not order; the snapshot drifts under contention. [Concurrency.Relaxed] drops the lock
 * and the two cells race independently with bounded drift; never throws.
 */
class HoltStat(
    /** Per-observation smoothing schedule used for both the level smoother and the trend smoother. */
    val alphaWeighting: DecayWeighting.Alpha,
    /** Per-observation smoothing schedule for the trend. Defaults to the level's [alphaWeighting]. */
    val betaWeighting: DecayWeighting.Alpha = alphaWeighting,
    /** Trend damping factor in `(0.0, 1.0]`; `1.0` is plain Holt, smaller values damp the trend. */
    val phi: Double = 1.0,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<HoltResult> {

    constructor(
        alpha: Double,
        beta: Double = alpha,
        phi: Double = 1.0,
        concurrency: Concurrency = Concurrency.None,
    ) : this(DecayWeighting.Alpha(alpha), DecayWeighting.Alpha(beta), phi, concurrency)

    init {
        require(phi > 0.0 && phi <= 1.0) { "phi must be in (0, 1], got $phi" }
    }

    /** Level smoothing factor; larger weights recent samples more. */
    val alpha: Double get() = alphaWeighting.alpha

    /** Trend smoothing factor; larger weights recent slopes more. */
    val beta: Double get() = betaWeighting.alpha

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val initialized = mode.newLong(0L)
    private val level = mode.newDouble(0.0)
    private val trend = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            if (initialized.addAndGet(1L) == 1L) {
                level.store(value)
                trend.store(0.0)
                return@guarded
            }
            val a = alphaWeighting.correction(weight)
            val b = betaWeighting.correction(weight)
            val prevLevel = level.load()
            val prevTrend = trend.load()
            val newLevel = a * value + (1.0 - a) * (prevLevel + phi * prevTrend)
            val newTrend = b * (newLevel - prevLevel) + (1.0 - b) * phi * prevTrend
            level.store(newLevel)
            trend.store(newTrend)
        }
    }

    override fun merge(values: HoltResult) = lock.guarded {
        // No principled stat-level merge of two independent Holt traces; take the other's snapshot
        // as the new state once we have anything to merge into. Useful for windowed-slot folds.
        if (initialized.addAndGet(1L) == 1L) {
            level.store(values.level)
            trend.store(values.trend)
        } else {
            level.store(0.5 * (level.load() + values.level))
            trend.store(0.5 * (trend.load() + values.trend))
        }
    }

    override fun reset() = lock.guarded {
        initialized.store(0L)
        level.store(0.0)
        trend.store(0.0)
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        HoltResult(level.load(), trend.load(), phi)
    }

    override fun create(concurrency: Concurrency?) = HoltStat(
        alphaWeighting = alphaWeighting,
        betaWeighting = betaWeighting,
        phi = phi,
        concurrency = concurrency ?: this.concurrency,
    )
}

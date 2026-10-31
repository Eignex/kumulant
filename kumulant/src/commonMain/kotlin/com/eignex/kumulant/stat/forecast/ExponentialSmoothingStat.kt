package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Additive vs multiplicative seasonal coupling. */
@Serializable
enum class SeasonalMode {
    /** Seasonal effect adds to the level: `value = level + season`. */
    Additive,

    /** Seasonal effect multiplies the level: `value = level * season`, requires non-negative inputs. */
    Multiplicative,
}

/**
 * Snapshot from an [ExponentialSmoothingStat]: level, trend, seasonal factors, and forecast machinery.
 * A stat without seasonality reports a single identity factor.
 */
@Serializable
@SerialName("ExponentialSmoothingResult")
data class ExponentialSmoothingResult(
    /** Smoothed level (one-step-ahead estimate at the current moment). */
    val level: Double,
    /** Smoothed per-update trend. */
    val trend: Double,
    /** Seasonal factors; length equals the configured period and wraps around at horizon h. */
    val seasons: List<Double>,
    /** Index `0..period-1` of the next seasonal slot to consume. */
    val currentSlot: Int,
    /** Damping factor applied to the trend each step (`1.0` for an undamped trend). */
    val phi: Double,
    /** Seasonal coupling. */
    val mode: SeasonalMode,
) : Result {
    init {
        // Validated here rather than in merge, because the slot indexes [seasons] on the stat's
        // update path: a decoded result carrying a negative slot merges without complaint and then
        // takes the *next* update out with an index-out-of-bounds, which no stat may throw.
        require(seasons.isNotEmpty()) { "seasons must not be empty" }
        require(currentSlot in seasons.indices) {
            "currentSlot must be in 0..${seasons.size - 1}; got $currentSlot"
        }
    }

    /** Length of the seasonal cycle; `1` when the stat has no seasonality. */
    val period: Int get() = seasons.size

    /**
     * Project [steps] updates ahead using the same recurrence shape as the stat. The trend
     * contribution is the damped sum `level + (phi + phi^2 + ... + phi^steps) * trend`, which is
     * `level + steps * trend` at `phi == 1.0`; the seasonal contribution wraps around [seasons].
     */
    fun forecast(steps: Int): Double {
        requireForecastSteps(steps)
        if (steps == 0) return level
        val trendPart = level + dampedTrendSum(phi, steps) * trend
        // Long: `currentSlot + steps - 1` overflows Int at a large horizon, and the renormalisation
        // below keeps the wrapped value in range rather than throwing, so the wrong slot is picked
        // silently whenever period is not a power of two. dampedTrendSum has a closed form precisely
        // so a large horizon is cheap, which makes one reachable by design.
        val ahead = currentSlot.toLong() + steps.toLong() - 1L
        val seasonIndex = ((ahead % period + period) % period).toInt()
        val seasonFactor = seasons[seasonIndex]
        return when (mode) {
            SeasonalMode.Additive -> trendPart + seasonFactor
            SeasonalMode.Multiplicative -> trendPart * seasonFactor
        }
    }
}

/**
 * Exponential smoothing with a damped trend and optional seasonality: Holt's method at
 * [period] `1`, Holt-Winters with a seasonal cycle of [period] updates otherwise.
 *
 * Additive recurrence (with `a = correction(alpha, weight)` and analogous `b`, `g`,
 * and `k` the current seasonal-slot index):
 *
 * ```
 * sOld   = seasons[k]
 * prev   = level
 * level  = a * (value - sOld) + (1 - a) * (prev + phi * trend)
 * trend  = b * (level - prev) + (1 - b) * phi * trend
 * seasons[k] = g * (value - level) + (1 - g) * sOld
 * k      = (k + 1) mod period
 * ```
 *
 * Multiplicative is the same shape with subtraction replaced by division and addition by
 * multiplication; a zero season falls back to `1.0` and a zero level leaves the season as it was, so
 * neither path divides by zero. At [period] `1` the single season stays at its identity and the
 * recurrence reduces to Holt's level and trend.
 *
 * With `phi == 1.0` the trend is undamped; with `phi < 1.0` it is geometrically damped.
 * [com.eignex.kumulant.stat.decay.EwmaMeanStat] is the trend-free special case `beta == 0`.
 *
 * The first update seeds the level, a zero trend, and leaves the seasonal vector at its identity
 * (0 additive, 1 multiplicative); every later update runs the recurrence above. Nothing seeds the
 * seasonal vector separately, so the first cycle carries little seasonal signal.
 *
 * Merge adopts a remote state while empty and otherwise averages level, trend, and
 * seasonal factors, using the remote seasonal phase.
 * This is an approximate roll-up, not replay of the combined ordered observations.
 *
 * **Use cases:** smoothing a noisy series and producing short-horizon forecasts when a local trend,
 * a recurring cycle, or both are expected.
 *
 * **Memory:** O(period); three scalar cells plus a season array plus a lock.
 *
 * **Update:** O(1); single seasonal slot touched per update.
 *
 * **Concurrency:** Order-dependent recurrence, same model as
 * [com.eignex.kumulant.stat.decay.EwmaMeanStat]. [Concurrency.Strict] and
 * [Concurrency.HighWrite] lock the body so each update is atomic but the lock serialises
 * arrival, not order; [Concurrency.Relaxed] drops the lock and the level/trend/seasonal cells race
 * independently with bounded drift; never throws.
 */
class ExponentialSmoothingStat(
    /** Per-observation smoothing schedule for the level. */
    val alphaWeighting: DecayWeighting.Alpha,
    /** Per-observation smoothing schedule for the trend. Defaults to the level's [alphaWeighting]. */
    val betaWeighting: DecayWeighting.Alpha = alphaWeighting,
    /** Trend damping factor in `(0, 1]`; `1.0` is an undamped trend. */
    val phi: Double = StatDefaults.SMOOTHING_PHI,
    /** Length of the seasonal cycle in updates; `1` disables seasonality. */
    val period: Int = StatDefaults.SMOOTHING_PERIOD,
    /** Per-observation smoothing schedule for the seasonal vector. Defaults to the level's [alphaWeighting]. */
    val gammaWeighting: DecayWeighting.Alpha = alphaWeighting,
    /** Seasonal coupling; defaults to [SeasonalMode.Additive]. */
    val mode: SeasonalMode = StatDefaults.seasonalMode,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<ExponentialSmoothingResult> {

    constructor(
        alpha: Double,
        beta: Double = alpha,
        phi: Double = StatDefaults.SMOOTHING_PHI,
        period: Int = StatDefaults.SMOOTHING_PERIOD,
        gamma: Double = alpha,
        mode: SeasonalMode = StatDefaults.seasonalMode,
        concurrency: Concurrency = Concurrency.None,
    ) : this(
        DecayWeighting.Alpha(alpha),
        DecayWeighting.Alpha(beta),
        phi,
        period,
        DecayWeighting.Alpha(gamma),
        mode,
        concurrency,
    )

    init {
        require(period >= 1) { "period must be >= 1, got $period" }
        requirePhi(phi)
    }

    /** Level decay rate; each update steps the level by `1 - exp(-alpha * weight)`. */
    val alpha: Double get() = alphaWeighting.alpha

    /** Trend decay rate; each update steps the trend by `1 - exp(-beta * weight)`. */
    val beta: Double get() = betaWeighting.alpha

    /** Seasonal decay rate; each update steps the current season by `1 - exp(-gamma * weight)`. */
    val gamma: Double get() = gammaWeighting.alpha

    private val seasonal = period > 1
    private val streamMode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val initialized = streamMode.newLong(0L)
    private val level = streamMode.newDouble(0.0)
    private val trend = streamMode.newDouble(0.0)

    private val seasonIdentity: Double = if (mode == SeasonalMode.Multiplicative) 1.0 else 0.0
    private val seasons = streamMode.newDoubleArray(period) { seasonIdentity }
    private val slot = streamMode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Non-positive rather than merely inert: this order-dependent recurrence has no inverse, so
        // a downdate would run forwards as an ordinary observation. See Stat.
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            val seen = initialized.addAndGet(1L)
            val currentSlot = (slot.load() % period).toInt()
            if (seen == 1L) {
                level.store(value)
                trend.store(0.0)
                // Leave seasons at their identity (0 additive, 1 multiplicative) and advance the slot.
                slot.store((currentSlot + 1).toLong() % period)
                return@guarded
            }
            val a = alphaWeighting.correction(weight)
            val b = betaWeighting.correction(weight)
            val prevLevel = level.load()
            val prevTrend = trend.load()
            val sOld = seasons.load(currentSlot)
            val newLevel = when (mode) {
                SeasonalMode.Additive -> a * (value - sOld) + (1.0 - a) * (prevLevel + phi * prevTrend)

                SeasonalMode.Multiplicative -> {
                    val effectiveSOld = if (sOld == 0.0) 1.0 else sOld
                    a * (value / effectiveSOld) + (1.0 - a) * (prevLevel + phi * prevTrend)
                }
            }
            val newTrend = b * (newLevel - prevLevel) + (1.0 - b) * phi * prevTrend
            level.store(newLevel)
            trend.store(newTrend)
            if (seasonal) seasons.store(currentSlot, nextSeason(value, newLevel, sOld, weight))
            slot.store((currentSlot + 1).toLong() % period)
        }
    }

    private fun nextSeason(value: Double, newLevel: Double, sOld: Double, weight: Double): Double {
        val g = gammaWeighting.correction(weight)
        return when (mode) {
            SeasonalMode.Additive -> g * (value - newLevel) + (1.0 - g) * sOld
            SeasonalMode.Multiplicative -> if (newLevel == 0.0) sOld else g * (value / newLevel) + (1.0 - g) * sOld
        }
    }

    override fun validateMerge(values: ExponentialSmoothingResult) {
        require(values.seasons.size == period) { "merge seasons size ${values.seasons.size} != period $period" }
        // Additive factors sit around 0 and multiplicative ones around 1; their average is neither.
        require(values.mode == mode) { "merge seasonal mode ${values.mode} != $mode" }
    }

    override fun merge(values: ExponentialSmoothingResult, timestampNanos: Long) = lock.guarded {
        validateMerge(values)
        if (initialized.addAndGet(1L) == 1L) {
            level.store(values.level)
            trend.store(values.trend)
            values.seasons.forEachIndexed { i, s -> seasons.store(i, s) }
            slot.store(values.currentSlot.toLong())
        } else {
            level.store(0.5 * (level.load() + values.level))
            trend.store(0.5 * (trend.load() + values.trend))
            for (i in 0 until period) {
                seasons.store(i, 0.5 * (seasons.load(i) + values.seasons[i]))
            }
            // The factors just came from the incoming trace, so the phase has to come with them;
            // keeping the local slot would silently pair averaged factors with a mismatched phase.
            slot.store(values.currentSlot.toLong())
        }
    }

    override fun reset() = lock.guarded {
        initialized.store(0L)
        level.store(0.0)
        trend.store(0.0)
        slot.store(0L)
        val identity = seasonIdentity
        for (i in 0 until period) seasons.store(i, identity)
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        ExponentialSmoothingResult(
            level = level.load(),
            trend = trend.load(),
            seasons = List(period) { seasons.load(it) },
            currentSlot = (slot.load() % period).toInt(),
            phi = phi,
            mode = mode,
        )
    }

    override fun create(concurrency: Concurrency?) = ExponentialSmoothingStat(
        alphaWeighting = alphaWeighting,
        betaWeighting = betaWeighting,
        phi = phi,
        period = period,
        gammaWeighting = gammaWeighting,
        mode = mode,
        concurrency = concurrency ?: this.concurrency,
    )
}

package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
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

/** Snapshot from a [SeasonalSmoothingStat]: level, trend, seasonal factors, and forecast machinery. */
@Serializable
@SerialName("SeasonalSmoothingResult")
data class SeasonalSmoothingResult(
    /** Smoothed level. */
    val level: Double,
    /** Smoothed trend. */
    val trend: Double,
    /** Seasonal factors; length equals the configured period and wraps around at horizon h. */
    val seasons: List<Double>,
    /** Index `0..period-1` of the next seasonal slot to consume. */
    val currentSlot: Int,
    /** Trend damping factor in `(0, 1]`. */
    val phi: Double,
    /** Seasonal coupling. */
    val mode: SeasonalMode,
) : Result {
    /** Length of the seasonal cycle. */
    val period: Int get() = seasons.size

    /**
     * Project [steps] updates ahead using the same recurrence shape as the stat:
     * `trend` contribution is geometrically damped by `phi`, seasonal contribution
     * wraps around the [seasons] vector.
     */
    fun forecast(steps: Int): Double {
        require(steps >= 0) { "forecast steps must be >= 0, got $steps" }
        if (steps == 0) return level
        var phiPow = phi
        var sum = 0.0
        repeat(steps) {
            sum += phiPow
            phiPow *= phi
        }
        val trendPart = level + sum * trend
        val seasonIndex = ((currentSlot + steps - 1) % period + period) % period
        val seasonFactor = seasons[seasonIndex]
        return when (mode) {
            SeasonalMode.Additive -> trendPart + seasonFactor
            SeasonalMode.Multiplicative -> trendPart * seasonFactor
        }
    }
}

/**
 * Triple exponential smoothing (Holt-Winters): adds a seasonal component of [period] to
 * [HoltStat]'s level/trend recurrence. Supports additive and multiplicative seasonality.
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
 * multiplication. The first [period] updates seed the seasonal vector (additive: from
 * the residual `value - level`; multiplicative: from `value / level`, falling back to
 * `1.0` if `level == 0.0`).
 *
 * **Use cases:** short-horizon forecasting of streams with a recurring cycle on top
 * of a level and trend; pairs with [HoltStat] when no seasonal component is present.
 *
 * **Memory:** O(period); three scalar cells plus a season array plus a lock.
 *
 * **Update:** O(1); single seasonal slot touched per update.
 *
 * **Concurrency:** Order-dependent recurrence, same model as [HoltStat] and
 * [com.eignex.kumulant.stat.decay.EwmaMeanStat]. [Concurrency.Strict] and
 * [Concurrency.HighWrite] lock the body so each update is atomic;
 * [Concurrency.Relaxed] drops the lock and the level/trend/seasonal cells race independently
 * with bounded drift; never throws.
 */
class SeasonalSmoothingStat(
    /** Per-observation smoothing schedule for the level. */
    val alphaWeighting: DecayWeighting.Alpha,
    /** Per-observation smoothing schedule for the trend. */
    val betaWeighting: DecayWeighting.Alpha,
    /** Per-observation smoothing schedule for the seasonal vector. */
    val gammaWeighting: DecayWeighting.Alpha,
    /** Length of the seasonal cycle in updates. */
    val period: Int,
    /** Seasonal coupling; defaults to [SeasonalMode.Additive]. */
    val mode: SeasonalMode = SeasonalMode.Additive,
    /** Trend damping factor in `(0, 1]`; `1.0` is plain Holt-Winters. */
    val phi: Double = 1.0,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<SeasonalSmoothingResult> {

    constructor(
        alpha: Double,
        beta: Double,
        gamma: Double,
        period: Int,
        mode: SeasonalMode = SeasonalMode.Additive,
        phi: Double = 1.0,
        concurrency: Concurrency = Concurrency.None,
    ) : this(
        DecayWeighting.Alpha(alpha),
        DecayWeighting.Alpha(beta),
        DecayWeighting.Alpha(gamma),
        period,
        mode,
        phi,
        concurrency,
    )

    init {
        require(period >= 2) { "period must be >= 2, got $period" }
        require(phi > 0.0 && phi <= 1.0) { "phi must be in (0, 1], got $phi" }
    }

    /** Level smoothing factor. */
    val alpha: Double get() = alphaWeighting.alpha

    /** Trend smoothing factor. */
    val beta: Double get() = betaWeighting.alpha

    /** Seasonal smoothing factor. */
    val gamma: Double get() = gammaWeighting.alpha

    private val streamMode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val initialized = streamMode.newLong(0L)
    private val level = streamMode.newDouble(0.0)
    private val trend = streamMode.newDouble(0.0)
    private val seasonInit = streamMode.newDouble(if (mode == SeasonalMode.Multiplicative) 1.0 else 0.0)
    private val seasons = streamMode.newDoubleArray(period) { seasonInit.load() }
    private val slot = streamMode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.guarded {
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
        val g = gammaWeighting.correction(weight)
        val prevLevel = level.load()
        val prevTrend = trend.load()
        val sOld = seasons.load(currentSlot)
        val newLevel: Double
        val newSeason: Double
        when (mode) {
            SeasonalMode.Additive -> {
                newLevel = a * (value - sOld) + (1.0 - a) * (prevLevel + phi * prevTrend)
                newSeason = g * (value - newLevel) + (1.0 - g) * sOld
            }

            SeasonalMode.Multiplicative -> {
                val effectiveSOld = if (sOld == 0.0) 1.0 else sOld
                newLevel = a * (value / effectiveSOld) + (1.0 - a) * (prevLevel + phi * prevTrend)
                newSeason = if (newLevel == 0.0) sOld else g * (value / newLevel) + (1.0 - g) * sOld
            }
        }
        val newTrend = b * (newLevel - prevLevel) + (1.0 - b) * phi * prevTrend
        level.store(newLevel)
        trend.store(newTrend)
        seasons.store(currentSlot, newSeason)
        slot.store((currentSlot + 1).toLong() % period)
    }

    override fun merge(values: SeasonalSmoothingResult) = lock.guarded {
        require(values.seasons.size == period) { "merge seasons size ${values.seasons.size} != period $period" }
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
        }
    }

    override fun reset() = lock.guarded {
        initialized.store(0L)
        level.store(0.0)
        trend.store(0.0)
        slot.store(0L)
        val identity = if (mode == SeasonalMode.Multiplicative) 1.0 else 0.0
        for (i in 0 until period) seasons.store(i, identity)
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        SeasonalSmoothingResult(
            level = level.load(),
            trend = trend.load(),
            seasons = List(period) { seasons.load(it) },
            currentSlot = (slot.load() % period).toInt(),
            phi = phi,
            mode = mode,
        )
    }

    override fun create(concurrency: Concurrency?) = SeasonalSmoothingStat(
        alphaWeighting = alphaWeighting,
        betaWeighting = betaWeighting,
        gammaWeighting = gammaWeighting,
        period = period,
        mode = mode,
        phi = phi,
        concurrency = concurrency ?: this.concurrency,
    )
}

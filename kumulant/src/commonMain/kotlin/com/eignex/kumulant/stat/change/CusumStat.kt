package com.eignex.kumulant.stat.change

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.max
import kotlin.math.min

/** Snapshot from a two-sided [CusumStat] change-point detector. */
@Serializable
@SerialName("CusumResult")
data class CusumResult(
    /** In-control target value updates are compared against. */
    val target: Double,
    /** Reference value (allowance) `k`; deviations smaller than this are absorbed. */
    val referenceValue: Double,
    /** Decision threshold `h`; the alarm fires when the positive or negative cusum exceeds this. */
    val threshold: Double,
    /** Running positive-side cumulative sum, clipped at zero. */
    val cusumPositive: Double,
    /** Running negative-side cumulative sum, clipped at zero (non-positive value). */
    val cusumNegative: Double,
    /** True when the positive side has crossed [threshold]. */
    val alarmUp: Boolean,
    /** True when the negative side has crossed `-threshold`. */
    val alarmDown: Boolean,
) : Result {
    /** True when either side has crossed the threshold. */
    val alarm: Boolean get() = alarmUp || alarmDown
}

/**
 * Two-sided cumulative-sum (CUSUM) change-point detector. Tracks
 *
 * ```
 * S+_t = max(0, S+_{t-1} + (x_t - target - k))
 * S-_t = min(0, S-_{t-1} + (x_t - target + k))
 * ```
 *
 * and raises `alarmUp` when `S+ > h` or `alarmDown` when `-S- > h`. The reference
 * value [referenceValue] absorbs in-control variation; the threshold [threshold]
 * controls false-alarm rate. Standard rules of thumb are `k = 0.5 * sigma` and
 * `h = 4..5 * sigma` for unit-variance shifts.
 *
 * **Use cases:** detecting mean shifts in monitored signals (SPC, change-point
 * alarms on metrics, drift detection on model residuals).
 *
 * **Memory:** O(1) — two cells.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Coupled positive / negative cusum cells (category 3). The internal
 * lock keeps the multi-cell update consistent.
 */
class CusumStat(
    /** In-control target value to compare each input against. */
    val target: Double = 0.0,
    /** Reference value (allowance) absorbing in-control variation; must be `>= 0`. */
    val referenceValue: Double = 0.5,
    /** Alarm threshold for either side; must be `>= 0`. */
    val threshold: Double = 5.0,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<CusumResult> {

    init {
        require(referenceValue >= 0.0) { "referenceValue must be >= 0, got $referenceValue" }
        require(threshold >= 0.0) { "threshold must be >= 0, got $threshold" }
    }

    private val streamMode = concurrency.monotonicMode()
    private val lock = concurrency.serializedLock()
    private val cusumPos = streamMode.newDouble(0.0)
    private val cusumNeg = streamMode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        val deviation = value - target
        cusumPos.store(max(0.0, cusumPos.load() + deviation - referenceValue))
        cusumNeg.store(min(0.0, cusumNeg.load() + deviation + referenceValue))
    }

    override fun merge(values: CusumResult) = lock.withLock {
        // No exact recurrence-preserving merge; combine snapshots by averaging the two cusums.
        cusumPos.store(0.5 * (cusumPos.load() + values.cusumPositive))
        cusumNeg.store(0.5 * (cusumNeg.load() + values.cusumNegative))
    }

    override fun reset() = lock.withLock {
        cusumPos.store(0.0)
        cusumNeg.store(0.0)
    }

    override fun read(timestampNanos: Long) = lock.withLock {
        val pos = cusumPos.load()
        val neg = cusumNeg.load()
        CusumResult(
            target = target,
            referenceValue = referenceValue,
            threshold = threshold,
            cusumPositive = pos,
            cusumNegative = neg,
            alarmUp = pos > threshold,
            alarmDown = -neg > threshold,
        )
    }

    override fun create(concurrency: Concurrency?) =
        CusumStat(target, referenceValue, threshold, concurrency ?: this.concurrency)
}

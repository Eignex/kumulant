package com.eignex.kumulant.stat.change

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.monotonicMode
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
 * **Memory:** O(1); two cells.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** The positive and negative cumulative sums are independent recurrences;
 * each cell is updated via a single-cell CAS loop (category 2). No lock; concurrent
 * updates retry their own cell's CAS without contending across sides and never throw.
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
    private val cusumPos = streamMode.newDouble(0.0)
    private val cusumNeg = streamMode.newDouble(0.0)

    /**
     * Whether anything has landed yet, which the two cells cannot tell us on their own.
     *
     * `cusumPos` clamps at zero and `cusumNeg` at zero from below, so "no observation yet" and "the
     * drift has walked back to zero" are the same pair of numbers. [merge] has to distinguish them.
     */
    private val initialized = streamMode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        val deviation = value - target
        while (true) {
            val prev = cusumPos.load()
            val next = max(0.0, prev + deviation - referenceValue)
            if (cusumPos.compareAndSet(prev, next)) break
        }
        while (true) {
            val prev = cusumNeg.load()
            val next = min(0.0, prev + deviation + referenceValue)
            if (cusumNeg.compareAndSet(prev, next)) break
        }
        initialized.store(1L)
    }

    override fun merge(values: CusumResult) {
        // Adopt the snapshot verbatim while empty, matching RecursiveVarianceStat.merge, HoltStat.merge
        // and SeasonalSmoothingStat.merge. Averaging against a fresh stat's 0.0 halved the incoming
        // drift, and halving the drift is not a rounding matter here: `alarmUp` compares the cumulative
        // sum against `threshold`, so a coordinator that merged a worker's snapshot needed twice the
        // real shift before it would fire. The baselines were merged exactly, which hid it further.
        val empty = initialized.load() == 0L
        while (true) {
            val prev = cusumPos.load()
            val next = if (empty) values.cusumPositive else 0.5 * (prev + values.cusumPositive)
            if (cusumPos.compareAndSet(prev, next)) break
        }
        while (true) {
            val prev = cusumNeg.load()
            val next = if (empty) values.cusumNegative else 0.5 * (prev + values.cusumNegative)
            if (cusumNeg.compareAndSet(prev, next)) break
        }
        initialized.store(1L)
    }

    override fun reset() {
        cusumPos.store(0.0)
        cusumNeg.store(0.0)
        initialized.store(0L)
    }

    override fun read(timestampNanos: Long): CusumResult {
        val pos = cusumPos.load()
        val neg = cusumNeg.load()
        return CusumResult(
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

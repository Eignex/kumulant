package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock
import kotlin.math.abs

/**
 * Constant-memory adaptive median and median absolute deviation estimates.
 *
 * The first observation seeds the center and zero deviation. Each later update moves
 * both estimates toward their targets by at most half [stepSize], clamped to avoid
 * overshooting. The deviation target is the absolute residual against the pre-update
 * center. Constant steps forget earlier centers gradually; these estimates track a
 * changing distribution rather than reconstructing its historical MAD.
 *
 * Positive weights each count as one observation, as in
 * [com.eignex.kumulant.stat.quantile.FrugalQuantileStat]. Zero and non-finite weights
 * are inert; negative weights throw because the recurrence has no inverse.
 * Choose [stepSize] in the input's units: larger steps adapt faster with more jitter.
 *
 * **Use cases:** lightweight robust center/scale monitoring where bounded state and
 * adaptation matter more than quantile accuracy. Use [MadStat] for digest-based
 * historical estimates, observation weights, and digest-backed merging.
 *
 * **Memory:** O(1); two estimates, an initialization flag, and a lock.
 *
 * **Update:** O(1); two bounded adjustments, with no per-update allocation.
 *
 * **Concurrency:** Body locked under any concurrent level. Updates, reads, and resets
 * see a coherent pair; estimates remain sensitive to observation order. Merge adopts
 * a nonempty remote pair while empty and otherwise averages both estimates equally,
 * without reconstructing the combined distribution or its observation weights.
 */
class FrugalMadStat(
    /** Full step size in input units; each median adjustment is bounded by half this value. */
    val stepSize: Double = StatDefaults.FRUGAL_QUANTILE_STEP_SIZE,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<MadResult> {

    init {
        require(stepSize.isFinite() && stepSize > 0.0) { "stepSize must be finite and positive" }
    }

    private val lock = concurrency.serializedLock()
    private val halfStep = stepSize / 2.0
    private var initialized = false
    private var median = Double.NaN
    private var deviation = Double.NaN

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        require(weight > 0.0) { "FrugalMadStat cannot downdate a negative weight" }
        lock.guarded {
            if (!initialized) {
                median = value
                deviation = 0.0
                initialized = true
            } else {
                val residual = abs(value - median)
                median += (value - median).coerceIn(-halfStep, halfStep)
                deviation += (residual - deviation).coerceIn(-halfStep, halfStep)
            }
        }
    }

    override fun read(timestampNanos: Long): MadResult = lock.guarded { MadResult(median, deviation) }

    override fun merge(values: MadResult) = lock.guarded {
        if (values.median.isNaN()) return@guarded
        val incomingDeviation = if (values.mad.isNaN()) 0.0 else values.mad
        if (!initialized) {
            median = values.median
            deviation = incomingDeviation
            initialized = true
        } else {
            median = median / 2.0 + values.median / 2.0
            deviation = deviation / 2.0 + incomingDeviation / 2.0
        }
    }

    override fun reset() = lock.guarded {
        initialized = false
        median = Double.NaN
        deviation = Double.NaN
    }

    override fun create(concurrency: Concurrency?) = FrugalMadStat(stepSize, concurrency ?: this.concurrency)
}

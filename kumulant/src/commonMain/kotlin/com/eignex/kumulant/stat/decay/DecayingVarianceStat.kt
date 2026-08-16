package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.currentTimeNanos
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.math.sqrt
import kotlin.time.Duration

/** Snapshot of an exponentially time-decayed weighted variance at [timestampNanos]. */
@Serializable
@SerialName("DecayingVarianceResult")
data class DecayingVarianceResult(
    /** Time-decayed weighted running mean. */
    val mean: Double,
    /** Time-decayed weighted running variance. */
    val variance: Double,
    /** Effective weight of observations still contributing (decays with time). */
    override val totalWeights: Double,
    /** Wall-clock timestamp (nanoseconds) at which the snapshot was taken. */
    val timestampNanos: Long,
) : HasObservationCount {
    /** Square root of [variance]. */
    val stdDev: Double get() = sqrt(variance)
}

/**
 * Exponentially decaying weighted variance over the recent time window.
 *
 * Holds a Welford-style accumulator `(W, mean, M2)` whose effective weight and
 * second-central-moment are decayed in lockstep with elapsed wall-clock time.
 * Each update advances the landmark to the event timestamp, decays `W` and `M2`
 * by `exp(-alpha*Deltat)`, then applies the standard increment
 * `M2 += w*delta*(value - meanNew)`. This avoids the catastrophic cancellation
 * of the `E[X^2] - E[X]^2` form when `stdDev << |mean|`.
 *
 * **Use cases:** recency-biased dispersion (rolling latency variance, recent
 * variance for control charts). Reach for this over [com.eignex.kumulant.stat.summary.VarianceStat] when older
 * observations should fade.
 *
 * **Memory:** O(1); landmark + three doubles plus a lock.
 *
 * **Update:** O(1) per observation; one `exp()` decay + Welford increment.
 *
 * **Concurrency:** Body locked under any concurrent [Concurrency] level
 * (no-op under [Concurrency.None]). The multi-cell decay-then-Welford
 * transition cannot survive lock-free CAS; see *Why locked under Relaxed*
 * below. Exact under every level up to floating-point reorder ULPs.
 */
class DecayingVarianceStat(
    /** Time-decay schedule applied to past contributions. */
    val weighting: DecayWeighting.HalfLife,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<DecayingVarianceResult> {

    constructor(halfLife: Duration, concurrency: Concurrency = Concurrency.None) :
        this(DecayWeighting.HalfLife(halfLife), concurrency)

    /** Wall-clock half-life of past contributions. */
    val halfLife: Duration get() = weighting.halfLife
    private val alpha = weighting.alpha

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.serializedLock()
    private val landmarkNanos = mode.newLong(currentTimeNanos())
    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val m2 = mode.newDouble(0.0)

    /**
     * Decay the accumulator forward to [t] and return the factor to apply to an observation
     * stamped there.
     *
     * A timestamp *behind* the landmark cannot be decayed to: `exp(-alpha*(t - landmark))` has a
     * positive exponent, so it would inflate `W` and `M2` rather than shrink them, overflow both
     * to `Infinity` past ~1024 half-lives, and drag the landmark backwards so nothing recovers.
     * An out-of-order stamp is a late arrival, not a rewind, so the landmark holds and the sample
     * is discounted by how late it is - which is what [DecayingSumStat] already does with its
     * `exp(alpha*dt)` for a negative `dt`.
     */
    private fun advanceTo(t: Long): Double {
        val priorLandmark = landmarkNanos.load()
        if (t == priorLandmark) return 1.0
        // While empty the landmark snaps to the observation, in either direction: it starts at
        // wall-clock construction time, and a replay stream numbering from its own epoch is
        // legitimately far behind that. Only once there is history to protect does a backwards
        // stamp mean a late arrival.
        val priorW = totalWeights.load()
        if (priorW == 0.0) {
            landmarkNanos.store(t)
            return 1.0
        }
        if (t < priorLandmark) return exp(-alpha * (priorLandmark - t).toDouble())
        val decay = exp(-alpha * (t - priorLandmark).toDouble())
        totalWeights.store(priorW * decay)
        m2.store(m2.load() * decay)
        landmarkNanos.store(t)
        return 1.0
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Both before the lock: a dropped observation neither contends for it nor moves the landmark.
        if (weight <= 0.0 || weight.isNaN()) return // a zero weight is a no-op; see Stat
        lock.guarded {
            val scaledWeight = weight * advanceTo(timestampNanos)
            if (scaledWeight <= 0.0) return@guarded // the sample is so late its weight underflowed away
            val priorW = totalWeights.load()
            val nextW = priorW + scaledWeight
            val priorMean = mean.load()
            val delta = value - priorMean
            val newMean = priorMean + delta * scaledWeight / nextW
            totalWeights.store(nextW)
            mean.store(newMean)
            m2.add(scaledWeight * delta * (value - newMean))
        }
    }

    override fun read(timestampNanos: Long): DecayingVarianceResult = lock.guarded {
        val landmark = landmarkNanos.load()
        val priorW = totalWeights.load()
        val priorM2 = m2.load()
        val w: Double
        val decayedM2: Double
        // A read behind the landmark gets the state as of the landmark rather than an inflated one,
        // for the same reason advanceTo refuses to rewind.
        if (priorW == 0.0 || timestampNanos <= landmark) {
            w = priorW
            decayedM2 = priorM2
        } else {
            val decay = exp(-alpha * (timestampNanos - landmark).toDouble())
            w = priorW * decay
            decayedM2 = priorM2 * decay
        }
        val variance = if (w > 0.0) decayedM2 / w else 0.0
        DecayingVarianceResult(mean.load(), variance, w, timestampNanos)
    }

    override fun merge(values: DecayingVarianceResult) = lock.guarded {
        if (values.totalWeights <= 0.0) return@guarded
        val target = maxOf(landmarkNanos.load(), values.timestampNanos)
        advanceTo(target)
        val remoteDecay = exp(-alpha * (target - values.timestampNanos).toDouble())
        val remoteW = values.totalWeights * remoteDecay
        val remoteM2 = values.variance * remoteW
        val w1 = totalWeights.load()
        val nextW = w1 + remoteW
        if (nextW == 0.0) return@guarded
        val priorMean = mean.load()
        val delta = values.mean - priorMean
        mean.store(priorMean + delta * remoteW / nextW)
        totalWeights.store(nextW)
        m2.add(remoteM2 + delta * delta * w1 * remoteW / nextW)
    }

    override fun reset() = lock.guarded {
        landmarkNanos.store(currentTimeNanos())
        totalWeights.store(0.0)
        mean.store(0.0)
        m2.store(0.0)
    }

    override fun create(concurrency: Concurrency?) = DecayingVarianceStat(weighting, concurrency ?: this.concurrency)
}

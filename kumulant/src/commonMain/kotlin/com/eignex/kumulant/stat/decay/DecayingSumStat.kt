package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.additiveMode
import com.eignex.kumulant.stream.currentTimeNanos
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.time.Duration

/** Snapshot of an exponentially time-decayed sum at [timestampNanos]. */
@Serializable
@SerialName("DecayingSumResult")
data class DecayingSumResult(
    /** Time-decayed sum at [timestampNanos]. */
    val sum: Double,
    /** Wall-clock timestamp (nanoseconds) at which the snapshot was taken. */
    val timestampNanos: Long,
) : Result

/**
 * Exponentially decaying sum driven by wall-clock elapsed time.
 *
 * `S(t) = Sum v_i * w_i * exp(-alpha*(t - t_i))` with `alpha = ln(2)/halfLife`.
 *
 * Internally uses landmark rotation to keep the stored accumulator in a
 * bounded numerical range even after many half-lives of activity.
 *
 * **Use cases:** time-windowed event totals (requests in the last 30s),
 * recency-weighted aggregation for monitoring dashboards. The core time-decay
 * primitive; pair with another [DecayingSumStat] for ratios via
 * [DecayingMeanStat], or with `ln(2)/halfLife` for per-second rates via
 * [com.eignex.kumulant.stat.rate.DecayingRateStat].
 *
 * **Memory:** O(1); one epoch (`(landmark, accumulator)`) at a time.
 *
 * **Update:** O(1) per observation; one `exp()` + one atomic add. Epoch
 * rotation fires at most once per `ROTATION_HALF_LIVES` half-lives and is
 * O(1) amortised.
 *
 * **Concurrency:** Lock-free under every [Concurrency] level. Updates discount
 * to the current epoch and atomic-add; epoch rotation is a CAS swap of the
 * epoch reference, losers retry against the new epoch.
 * [Concurrency.HighWrite] switches the accumulator cell to a striped adder.
 */
class DecayingSumStat(
    /** Time-decay schedule applied to past contributions. */
    val weighting: DecayWeighting.HalfLife,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<DecayingSumResult> {

    constructor(halfLife: Duration, concurrency: Concurrency = Concurrency.None) :
        this(DecayWeighting.HalfLife(halfLife), concurrency)

    /** Wall-clock half-life of past contributions. */
    val halfLife: Duration get() = weighting.halfLife
    private val alpha = weighting.alpha
    private val rotationThresholdNanos = weighting.halfLife.inWholeNanoseconds * ROTATION_HALF_LIVES

    private class Epoch(val landmarkNanos: Long, val accumulator: StreamDouble)

    private val mode = concurrency.additiveMode()
    private val epochRef = mode.newReference(
        Epoch(currentTimeNanos(), mode.newDouble(0.0)),
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        while (true) {
            val epoch = epochRef.load()
            if (timestampNanos - epoch.landmarkNanos > rotationThresholdNanos) {
                tryRotateEpoch(epoch, timestampNanos)
                continue
            }
            val dt = timestampNanos - epoch.landmarkNanos
            epoch.accumulator.add(value * weight * exp(alpha * dt))
            return
        }
    }

    private fun tryRotateEpoch(old: Epoch, now: Long) {
        val dt = now - old.landmarkNanos
        val carried = old.accumulator.load() * exp(-alpha * dt)
        epochRef.compareAndSet(old, Epoch(now, mode.newDouble(carried)))
    }

    override fun read(timestampNanos: Long): DecayingSumResult {
        val epoch = epochRef.load()
        val dt = (timestampNanos - epoch.landmarkNanos).toDouble()
        val sum = epoch.accumulator.load() * exp(-alpha * dt)
        return DecayingSumResult(sum, timestampNanos)
    }

    override fun merge(values: DecayingSumResult) {
        if (values.sum == 0.0) return
        while (true) {
            val epoch = epochRef.load()
            val now = values.timestampNanos
            if (now - epoch.landmarkNanos <= rotationThresholdNanos) {
                val dt = (now - epoch.landmarkNanos).toDouble()
                epoch.accumulator.add(values.sum * exp(alpha * dt))
                break
            }
            tryRotateEpoch(epoch, now)
        }
    }

    override fun reset() {
        val current = epochRef.load()
        epochRef.compareAndSet(current, Epoch(currentTimeNanos(), mode.newDouble(0.0)))
    }

    override fun create(concurrency: Concurrency?) = DecayingSumStat(weighting, concurrency ?: this.concurrency)

    private companion object {
        const val ROTATION_HALF_LIVES = 50L
    }
}

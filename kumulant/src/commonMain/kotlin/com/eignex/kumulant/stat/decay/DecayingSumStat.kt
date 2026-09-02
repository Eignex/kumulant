package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
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

    // Saturate rather than wrap: halfLife * 50 overflows Long above ~2135 days, and a negative
    // threshold makes the rotation test below true for every dt, so update() would rotate the
    // epoch, retry, find dt == 0 still above the threshold, and spin forever. A half-life that
    // long needs no rotation at all, so Long.MAX_VALUE (never rotate) is also the right answer.
    private val rotationThresholdNanos = weighting.halfLife.inWholeNanoseconds.let { halfLifeNanos ->
        if (halfLifeNanos > Long.MAX_VALUE / ROTATION_HALF_LIVES) {
            Long.MAX_VALUE
        } else {
            halfLifeNanos * ROTATION_HALF_LIVES
        }
    }

    private class Epoch(val landmarkNanos: Long, val accumulator: StreamDouble)

    private val mode = concurrency.additiveMode()
    private val epochRef = mode.newReference(
        Epoch(currentTimeNanos(), mode.newDouble(0.0)),
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Return before the rotation check, so a zero-weight observation cannot move the epoch
        // landmark. Also covers DecayingMeanStat and DecayingRateStat, built from this stat. See Stat.
        if (weight.isInertWeight()) return
        while (true) {
            val epoch = epochRef.load()
            // While empty the landmark snaps back to the observation: it starts at process uptime, and
            // a replay stream numbering from its own epoch is legitimately far behind that. Left alone,
            // `exp(alpha*dt)` for so negative a dt flushes the contribution to zero and read's matching
            // positive exponent then multiplies that zero by an infinity, so the stat reports NaN.
            // DecayingVarianceStat.advanceTo snaps for the same reason. Only backwards, and only while
            // there is no sum to protect: forwards is what the rotation below is for, and once there is
            // history a stamp behind the landmark is a late arrival rather than a rewind.
            if (timestampNanos < epoch.landmarkNanos && epoch.accumulator.load() == 0.0) {
                epochRef.compareAndSet(epoch, Epoch(timestampNanos, mode.newDouble(0.0)))
                continue
            }
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
        val next = Epoch(now, mode.newDouble(0.0))
        if (!epochRef.compareAndSet(old, next)) return
        // Drain the retired accumulator rather than snapshotting it: an update that loaded the old
        // epoch and adds after the swap would otherwise land in a cell nothing reads again. Taking the
        // residue out before folding it in means a racing add is either still there for the next pass
        // or already counted here, never both, so no observation is dropped. The loop is bounded by
        // the writers that were in flight when the swap landed.
        val factor = exp(-alpha * dt)
        while (true) {
            val residue = old.accumulator.load()
            if (residue == 0.0) return
            old.accumulator.add(-residue)
            next.accumulator.add(residue * factor)
        }
    }

    override fun read(timestampNanos: Long): DecayingSumResult {
        val epoch = epochRef.load()
        val stored = epoch.accumulator.load()
        val dt = (timestampNanos - epoch.landmarkNanos).toDouble()
        return DecayingSumResult(decayTo(stored, dt), timestampNanos)
    }

    /**
     * Apply the read-time decay factor without letting the two ends of the exponent meet as
     * `0.0 * Infinity`. An accumulator flushed to zero by an underflowing `exp(alpha*dt)` on the
     * update side stays zero here, and one that overflowed stays infinite, rather than both
     * turning into a NaN that every later read inherits.
     */
    private fun decayTo(stored: Double, dt: Double): Double {
        if (stored == 0.0) return 0.0
        return stored * exp(-alpha * dt)
    }

    /**
     * The accumulator as stored, with its landmark, before the read-time decay is applied. Two
     * sums sharing a weighting and an update stream share a landmark, so their ratio is the same
     * ratio the decayed values would give - and it survives the decay factor underflowing to
     * zero, which the decayed values do not. [DecayingMeanStat] needs that to keep reporting a
     * mean once the shared factor has flushed both of its sums to `0.0`.
     */
    internal class Undecayed(val landmarkNanos: Long, val sum: Double)

    /** Single-load snapshot of the current epoch; see [Undecayed]. */
    internal fun undecayed(): Undecayed {
        val epoch = epochRef.load()
        return Undecayed(epoch.landmarkNanos, epoch.accumulator.load())
    }

    override fun merge(values: DecayingSumResult, workspace: com.eignex.koblas.Workspace?) {
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
        // Retried, unlike a single attempt: losing the race to a concurrent rotation would drop the
        // reset silently and leave the stat carrying everything it had accumulated.
        while (true) {
            val current = epochRef.load()
            if (epochRef.compareAndSet(current, Epoch(currentTimeNanos(), mode.newDouble(0.0)))) return
        }
    }

    override fun create(concurrency: Concurrency?) = DecayingSumStat(weighting, concurrency ?: this.concurrency)

    private companion object {
        const val ROTATION_HALF_LIVES = 50L
    }
}

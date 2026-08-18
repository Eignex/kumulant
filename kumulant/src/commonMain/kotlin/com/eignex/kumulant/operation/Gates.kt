package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.math.deriveChildSeed
import com.eignex.kumulant.math.splitmix64
import com.eignex.kumulant.stream.monotonicMode

/**
 * Counts updates and lets every `every`-th one through.
 *
 * A wrapper holding one of these must call [reset] from its own `reset`: `Stat.reset` promises the
 * equivalent of a fresh stat, and `by delegate` forwards `reset` straight past the gate, leaving a reset
 * stat to forward on its first update rather than its `every`-th.
 *
 * A wrapper must also drop an inert weight before calling [pass]. The gate's phase is state, so
 * counting an observation that carries no multiplicity would shift which later updates survive - see
 * [Stat][com.eignex.kumulant.core.Stat] for the guarantee that makes that wrong.
 *
 * The counter resolves through [monotonicMode] so a gate under [Concurrency.None] costs a plain
 * increment rather than an atomic read-modify-write.
 */
internal class ThrottleGate(private val every: Int, concurrency: Concurrency) {
    init {
        checkEvery(every)
    }

    private val tick = concurrency.monotonicMode().newLong(0L)

    /** True when this update is the one to forward. */
    fun pass(): Boolean = tick.addAndGet(1L) % every == 0L

    /** Clears the phase, so the next update is counted as the first. */
    fun reset() = tick.store(0L)
}

/**
 * Lets a Bernoulli-sampled fraction of updates through.
 *
 * Counter-based rather than backed by a [kotlin.random.Random]: the draw is `splitmix64(seed + n)` for
 * the gate's n-th update, with `n` resolved through [monotonicMode]. A `Random` is a mutable object with
 * no atomicity, and this gate is read from every thread that updates the stat and is *copied* by
 * [childSeed] into every window slice, so a shared one would be mutated concurrently on both axes.
 * Deriving each draw from a counter makes the gate as thread-safe as the cell underneath it, and costs
 * a plain increment plus a mix under [Concurrency.None].
 *
 * The same shape as [ThrottleGate], and it carries the same two obligations on the wrapper: call
 * [reset] from the wrapper's `reset`, and drop an inert weight before calling [pass].
 *
 * The predicate is `<` rather than `<=`, which is what makes `rate = 0.0` reject everything;
 * [unitDraw] returns a value strictly below `1.0`, which is what makes `rate = 1.0` accept everything.
 */
internal class SampleGate(private val rate: Double, private val seed: Long, concurrency: Concurrency) {
    init {
        checkRate(rate)
    }

    private val mode = concurrency.monotonicMode()
    private val draws = mode.newLong(0L)
    private val copies = mode.newLong(0L)

    /** True when this update survives the sampling draw. */
    fun pass(): Boolean = unitDraw(seed + draws.addAndGet(1L)) < rate

    /** Clears the draw counter, so the gate replays its sequence from the start. */
    fun reset() = draws.store(0L)

    /** A seed for a copy of the owning stat, distinct on every call; see [deriveChildSeed]. */
    fun childSeed(): Long = deriveChildSeed(seed, copies.addAndGet(1L))
}

/**
 * Map 64 mixed bits onto `[0, 1)`.
 *
 * The top 53 bits, because that is the width of a `Double`'s significand: taking fewer would leave
 * representable values the draw can never produce, and taking more would round up to exactly `1.0` and
 * break the `rate = 1.0` case in [SampleGate].
 */
private fun unitDraw(state: Long): Double = (splitmix64(state) ushr 11).toDouble() / (1L shl 53).toDouble()

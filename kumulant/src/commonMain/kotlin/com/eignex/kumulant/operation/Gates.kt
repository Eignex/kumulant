package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.monotonicMode
import kotlin.random.Random

/**
 * Counts updates and lets every `every`-th one through.
 *
 * Five wrappers - the four in [Sampling] plus the regression one - carried this identically: the same
 * `init` check, the same counter, the same `% every == 0L` test, and the same five-line comment on why
 * `reset` has to clear the phase as well as the delegate. That comment is the reason to have one gate:
 * `Stat.reset` promises the equivalent of a fresh stat, `by delegate` forwards `reset` straight past a
 * counter the wrapper owns, and a wrapper that forgets to clear it forwards on its first update instead
 * of its `every`-th. Five copies of a subtlety is five chances to omit it.
 *
 * The counter goes through [monotonicMode] rather than a raw `AtomicLong`, which is the other thing the
 * copies got wrong. Every other stateful wrapper in this package resolves its cells from the delegate's
 * [Concurrency]; these five were unconditionally atomic, so a throttled stat under [Concurrency.None]
 * paid an atomic read-modify-write per update where a lagged stat next to it paid a plain increment.
 * Correctness was never at stake - always-atomic is strictly safer - only the cost the concurrency
 * contract exists to let a caller decline.
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
 * Five wrappers spelled out `if (random.nextDouble() < rate)` behind the same `init` check. Less
 * substance than [ThrottleGate], but the predicate is worth pinning in one place: `<` rather than `<=`
 * is what makes `rate = 0.0` reject everything, and five copies is five chances for one to admit an
 * update the caller asked to drop entirely.
 */
internal class SampleGate(private val rate: Double, private val random: Random) {
    init {
        checkRate(rate)
    }

    /** True when this update survives the sampling draw. */
    fun pass(): Boolean = random.nextDouble() < rate
}

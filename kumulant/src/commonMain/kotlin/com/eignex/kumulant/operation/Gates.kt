package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.monotonicMode
import kotlin.random.Random

/**
 * Counts updates and lets every `every`-th one through.
 *
 * A wrapper holding one of these must call [reset] from its own `reset`: `Stat.reset` promises the
 * equivalent of a fresh stat, and `by delegate` forwards `reset` straight past the gate, leaving a reset
 * stat to forward on its first update rather than its `every`-th.
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
 * The predicate is `<` rather than `<=`, which is what makes `rate = 0.0` reject everything.
 */
internal class SampleGate(private val rate: Double, private val random: Random) {
    init {
        checkRate(rate)
    }

    /** True when this update survives the sampling draw. */
    fun pass(): Boolean = random.nextDouble() < rate
}

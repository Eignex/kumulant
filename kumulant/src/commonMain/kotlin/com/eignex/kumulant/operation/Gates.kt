package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.math.deriveChildSeed
import com.eignex.kumulant.math.splitmix64
import com.eignex.kumulant.stream.monotonicMode

// Wrappers must reset the gate and drop inert weights before pass() to preserve its phase.
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

// Counter-derived draws support concurrent updates and independent child streams.
// Wrappers must reset the gate and drop inert weights before pass() to preserve its phase.
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

// The top 53 bits fit the Double significand and keep the draw strictly below 1.
private fun unitDraw(state: Long): Double = (splitmix64(state) ushr 11).toDouble() / (1L shl 53).toDouble()

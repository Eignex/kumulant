package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.math.deriveChildSeed
import com.eignex.kumulant.math.splitmix64
import com.eignex.kumulant.stream.monotonicMode

internal fun checkEvery(every: Int) = require(every >= 1) { "throttle every must be >= 1, got $every" }

internal fun checkRate(rate: Double) = require(rate in 0.0..1.0) { "sample rate must be in [0, 1], got $rate" }

/** How a gated wrapper decides which updates reach its delegate; opened once per stat instance. */
internal sealed interface GatePolicy {
    fun open(concurrency: Concurrency): UpdateGate
}

internal class ThrottlePolicy(private val every: Int) : GatePolicy {
    init {
        checkEvery(every)
    }

    override fun open(concurrency: Concurrency): UpdateGate = ThrottleGate(every, concurrency)
}

internal class SamplePolicy(private val rate: Double, private val seed: Long) : GatePolicy {
    init {
        checkRate(rate)
    }

    override fun open(concurrency: Concurrency): UpdateGate = SampleGate(rate, seed, concurrency)
}

// Wrappers must reset the gate and drop inert weights before pass() to preserve its phase.
internal interface UpdateGate {
    /** True when this update is forwarded. */
    fun pass(): Boolean

    /** Clears the gate's phase, so the next update is counted as the first. */
    fun reset()

    /** The policy for an independent copy of the owning stat. */
    fun childPolicy(): GatePolicy
}

private class ThrottleGate(private val every: Int, concurrency: Concurrency) : UpdateGate {
    private val tick = concurrency.monotonicMode().newLong(0L)

    override fun pass(): Boolean = tick.addAndGet(1L) % every == 0L

    override fun reset() = tick.store(0L)

    override fun childPolicy(): GatePolicy = ThrottlePolicy(every)
}

// Counter-derived draws support concurrent updates and independent child streams.
private class SampleGate(private val rate: Double, private val seed: Long, concurrency: Concurrency) : UpdateGate {
    private val mode = concurrency.monotonicMode()
    private val draws = mode.newLong(0L)
    private val copies = mode.newLong(0L)

    override fun pass(): Boolean = unitDraw(seed + draws.addAndGet(1L)) < rate

    override fun reset() = draws.store(0L)

    // Distinct on every call; see deriveChildSeed.
    override fun childPolicy(): GatePolicy = SamplePolicy(rate, deriveChildSeed(seed, copies.addAndGet(1L)))
}

// The top 53 bits fit the Double significand and keep the draw strictly below 1.
private fun unitDraw(state: Long): Double = (splitmix64(state) ushr 11).toDouble() / (1L shl 53).toDouble()

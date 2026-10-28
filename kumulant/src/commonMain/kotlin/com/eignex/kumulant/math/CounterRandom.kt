package com.eignex.kumulant.math

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.monotonicMode
import kotlin.random.Random

// Each draw mixes seed + a counter whose storage follows the concurrency level.
// Concurrent draws interleave while preserving valid samples.
internal class CounterRandom(private val seed: Long, concurrency: Concurrency) : Random() {

    private val mode = concurrency.monotonicMode()
    private val draws = mode.newLong(0L)

    // Copies are counted apart from draws so that `reset` can restore the draw sequence without also
    // handing a later copy a seed an earlier one already used.
    private val copies = mode.newLong(0L)

    override fun nextBits(bitCount: Int): Int = splitmix64(seed + draws.addAndGet(1L)).toInt().takeUpperBits(bitCount)

    /** Restarts the draw sequence, so a reset stat draws what a fresh one would. */
    fun reset() = draws.store(0L)

    /** A seed for a copy of the owning stat, distinct on every call; see [deriveChildSeed]. */
    fun childSeed(): Long = deriveChildSeed(seed, copies.addAndGet(1L))
}

// The mask makes zero-bit requests yield zero despite masked shift distances.
private fun Int.takeUpperBits(bitCount: Int): Int = ushr(32 - bitCount) and (-bitCount shr 31)

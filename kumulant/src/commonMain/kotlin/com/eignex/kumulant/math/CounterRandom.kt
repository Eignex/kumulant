package com.eignex.kumulant.math

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.monotonicMode
import kotlin.random.Random

/**
 * A [Random] whose state is a counter rather than a mutable seed, so it is safe to draw from under any
 * [Concurrency] level.
 *
 * The n-th draw is `splitmix64(seed + n)`, with `n` resolved through
 * [monotonicMode][com.eignex.kumulant.stream.monotonicMode]. A stock `Random` is a mutable object with
 * no atomicity, so a stat that draws on its update path races on it as soon as two threads update at
 * once - and a lock is the wrong answer on a path whose whole cost is a draw. Here each draw is one
 * atomic increment and one mix, and under [Concurrency.None] the increment is plain.
 *
 * Subclassing [Random] rather than exposing a bespoke draw method is what keeps
 * [nextPoissonOne][com.eignex.kumulant.math.nextPoissonOne] and every other distribution in
 * `Distributions.kt` usable unchanged, including the ones that consume a variable number of draws.
 *
 * Draws interleave arbitrarily between threads, so a contended stat sees a different sequence than a
 * serial replay would - the same bounded drift every concurrent stat is allowed. Each individual draw
 * is well-formed whoever wins the increment, which is the property a shared `Random` cannot offer.
 */
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

/**
 * Keep the top [bitCount] bits of this `Int`, zeroing the rest, as [Random.nextBits] requires.
 *
 * The stdlib has this exact helper and keeps it internal, so it is restated here. The mask
 * `(-bitCount shr 31)` is what makes `bitCount = 0` yield `0` rather than an unshifted value: the sign
 * propagation gives `-1` for any positive count and `0` for zero.
 */
private fun Int.takeUpperBits(bitCount: Int): Int = ushr(32 - bitCount) and (-bitCount shr 31)

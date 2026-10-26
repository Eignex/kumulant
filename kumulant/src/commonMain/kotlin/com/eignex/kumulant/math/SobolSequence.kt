package com.eignex.kumulant.math

import kotlin.random.Random

internal const val SOBOL_BITS = 52
private const val SOBOL_PERIOD = 1L shl SOBOL_BITS
private const val SOBOL_MASK = SOBOL_PERIOD - 1
private const val SOBOL_SCALE = 1.0 / SOBOL_PERIOD

/**
 * Sobol points in Gray-code order using the Joe-Kuo D(6) direction numbers.
 *
 * A non-null [scrambleSeed] applies a left linear matrix scramble and digital shift, independently
 * per coordinate. Null selects the unscrambled sequence, whose first point is zero. Use independent
 * seeds to estimate integration error, and powers of two from the start for balanced sample sets.
 * Skipping or thinning points can destroy that balance. The finite sequence has 2^52 points.
 *
 * Each coordinate's scramble depends on its global dimension and seed, so coordinate and sample
 * prefixes agree across differently sized engines. [dimensionOffset] selects a contiguous projection
 * of the same sequence, useful when constructing independent-coordinate blocks incrementally.
 * Engines own mutable position and scratch state and are single-threaded.
 *
 * @property dimensions Number of coordinates per point, in 1..[MAX_DIMENSIONS].
 * @property scrambleSeed Seed for LMS plus digital-shift scrambling, or null for no scrambling.
 * @property dimensionOffset Zero-based first global coordinate; offset + dimensions must not exceed the limit.
 */
class SobolSequence(val dimensions: Int, val scrambleSeed: Long? = 0L, val dimensionOffset: Int = 0) {
    private val directions: Array<LongArray>
    private val shifts: LongArray
    private val state: LongArray
    private var index = 0L

    init {
        require(dimensions in 1..MAX_DIMENSIONS && dimensionOffset in 0..MAX_DIMENSIONS - dimensions)
        directions = Array(dimensions) { sobolDirections(dimensionOffset + it) }
        shifts = LongArray(dimensions)
        if (scrambleSeed != null) {
            val random = Random(scrambleSeed)
            repeat(dimensionOffset * (SOBOL_BITS + 1)) { random.nextLong() }
            for (coordinate in 0 until dimensions) {
                val rows = LongArray(SOBOL_BITS) { row ->
                    val bit = SOBOL_BITS - 1 - row
                    val preceding = SOBOL_MASK xor ((1L shl (bit + 1)) - 1)
                    (random.nextLong() and preceding) or (1L shl bit)
                }
                for (column in 0 until SOBOL_BITS) {
                    val original = directions[coordinate][column]
                    var scrambled = 0L
                    for (row in 0 until SOBOL_BITS) {
                        val parity = (rows[row] and original).countOneBits() and 1
                        scrambled = scrambled or (parity.toLong() shl (SOBOL_BITS - 1 - row))
                    }
                    directions[coordinate][column] = scrambled
                }
                shifts[coordinate] = random.nextLong() and SOBOL_MASK
            }
        }
        state = shifts.copyOf()
    }

    /** Fill and return [out] with the next uniform point in [0, 1), advancing once. */
    fun nextPoint(out: DoubleArray = DoubleArray(dimensions)): DoubleArray = next(out, normal = false)

    /**
     * Fill and return [out] with the next standard-normal point, advancing once.
     * Uses AS 241's inverse normal CDF on 52-bit cell midpoints, so endpoint values stay finite.
     * Normal coordinates target unit marginal variance; these quasi-random points are not IID draws.
     */
    fun nextNormal(out: DoubleArray = DoubleArray(dimensions)): DoubleArray = next(out, normal = true)

    /** Set the next point's zero-based position, in [0, 2^52), including backward resets. */
    fun skipTo(position: Long) {
        require(position in 0 until SOBOL_PERIOD)
        index = position
        val gray = position xor (position ushr 1)
        for (coordinate in state.indices) {
            var value = shifts[coordinate]
            var remaining = gray
            while (remaining != 0L) {
                val column = remaining.countTrailingZeroBits()
                value = value xor directions[coordinate][column]
                remaining = remaining and (remaining - 1)
            }
            state[coordinate] = value
        }
    }

    private fun next(out: DoubleArray, normal: Boolean): DoubleArray {
        require(out.size == dimensions)
        check(index < SOBOL_PERIOD) { "Sobol sequence exhausted" }
        for (coordinate in out.indices) {
            val value = state[coordinate].toDouble()
            out[coordinate] = if (normal) standardNormalQuantile((value + 0.5) * SOBOL_SCALE) else value * SOBOL_SCALE
        }
        index++
        if (index < SOBOL_PERIOD) {
            val column = index.countTrailingZeroBits()
            for (coordinate in state.indices) state[coordinate] = state[coordinate] xor directions[coordinate][column]
        }
        return out
    }

    /** Supported Joe-Kuo direction-table extent. */
    companion object {
        /** Maximum global coordinate count, including any projection offset. */
        const val MAX_DIMENSIONS: Int = 1024
    }
}

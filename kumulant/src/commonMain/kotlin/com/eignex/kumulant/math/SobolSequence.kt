package com.eignex.kumulant.math

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
    private val delegate = com.eignex.util.random.SobolSequence(dimensions, scrambleSeed, dimensionOffset)

    /** Fill and return [out] with the next uniform point in [0, 1), advancing once. */
    fun nextPoint(out: DoubleArray = DoubleArray(dimensions)): DoubleArray = delegate.nextPoint(out)

    /** Fill and return [out] with the next finite standard-normal integration point, advancing once. */
    fun nextNormal(out: DoubleArray = DoubleArray(dimensions)): DoubleArray = delegate.nextNormal(out)

    /** Set the next point's zero-based position, in [0, 2^52), including backward resets. */
    fun skipTo(position: Long) = delegate.skipTo(position)

    /** Supported Joe-Kuo direction-table extent. */
    companion object {
        /** Maximum global coordinate count, including any projection offset. */
        const val MAX_DIMENSIONS: Int = com.eignex.util.random.SobolSequence.MAX_DIMENSIONS
    }
}

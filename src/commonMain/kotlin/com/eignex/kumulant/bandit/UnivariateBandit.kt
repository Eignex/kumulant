package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import kotlin.random.Random

/**
 * Online optimizer over a fixed set of unindexed arms. Each round the user calls
 * [choose] to select an arm, plays it externally, then reports the observed reward
 * via [update]. Arm state is owned by the [BanditPolicy], which decides both how to
 * evaluate arms and what sufficient statistic to track per arm.
 *
 * Implementations source all randomness from [random] so callers control the PRNG
 * (seeded for tests, shared for production, custom for special cases).
 */
interface UnivariateBandit<R : Result> {
    /** Pick an arm to play next; uses [random] for any sampling. */
    fun choose(): Int

    /** Score the arm at [armIndex] under the current policy. Parallels
     *  [com.eignex.kumulant.stat.regression.LinearPosterior.evaluate] for the
     *  univariate setting - useful for inspection, debugging, or building
     *  a custom selector on top of the per-arm scores. */
    fun evaluate(armIndex: Int): Double

    /** Fold a single observed reward [value] (with optional [weight]) into the arm at [armIndex]. */
    fun update(armIndex: Int, value: Double, weight: Double = 1.0)

    /** Batched [update]: applies one observation per arm/value pair. */
    fun updateAll(armIndices: IntArray, values: DoubleArray, weights: DoubleArray? = null) {
        require(armIndices.size == values.size) { "armIndices and values must have equal size" }
        require(weights == null || weights.size == values.size) { "weights must match values size" }
        for (i in armIndices.indices) update(armIndices[i], values[i], weights?.get(i) ?: 1.0)
    }

    /** Single source of randomness for [choose] and any policy-internal sampling. */
    val random: Random

    /** Materialise the current per-arm state for inspection or serialisation. */
    fun snapshot(): List<R>

    /** Per-arm snapshot at [armIndex]; default reads from [snapshot]. Implementations may
     *  override to avoid building the full list when only one arm is needed. */
    fun armResult(armIndex: Int): R = snapshot()[armIndex]
}

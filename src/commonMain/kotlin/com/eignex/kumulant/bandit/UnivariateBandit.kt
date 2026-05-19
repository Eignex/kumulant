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
 *
 * Sibling to [ContextualBandit]; population-state machinery lives on the joint
 * [Bandit] parent.
 */
interface UnivariateBandit<R : Result> : Bandit<R> {
    /** Pick an arm to play next; uses [random] for any sampling. */
    fun choose(): Int

    /** Score the arm at [armIndex] under the current policy. Useful for inspection,
     *  debugging, or building a custom selector on top of the per-arm scores. */
    fun evaluate(armIndex: Int): Double

    /** Fold a single observed reward [value] (with optional [weight]) into the arm at [armIndex]. */
    fun update(armIndex: Int, value: Double, weight: Double = 1.0)

    /** Batched [update]: applies one observation per arm/value pair. */
    fun updateAll(armIndices: IntArray, values: DoubleArray, weights: DoubleArray? = null) {
        require(armIndices.size == values.size) { "armIndices and values must have equal size" }
        require(weights == null || weights.size == values.size) { "weights must match values size" }
        for (i in armIndices.indices) update(armIndices[i], values[i], weights?.get(i) ?: 1.0)
    }

    override fun create(random: Random): UnivariateBandit<R>
}

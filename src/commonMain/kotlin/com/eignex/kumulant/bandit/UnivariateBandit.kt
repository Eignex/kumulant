package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result

/**
 * Online optimizer over a fixed set of unindexed arms. Each round the user calls [choose] to
 * select an arm, plays it externally, then reports the observed reward via [update]. Arm
 * state is owned by the [BanditPolicy], which decides both how to evaluate arms and what
 * sufficient statistic to track per arm.
 */
interface UnivariateBandit<R : Result> {
    fun choose(): Int
    fun update(armIndex: Int, value: Double, weight: Double = 1.0)
    fun updateAll(armIndices: IntArray, values: DoubleArray, weights: DoubleArray? = null) {
        require(armIndices.size == values.size) { "armIndices and values must have equal size" }
        require(weights == null || weights.size == values.size) { "weights must match values size" }
        for (i in armIndices.indices) update(armIndices[i], values[i], weights?.get(i) ?: 1.0)
    }

    val randomSeed: Int
    val maximize: Boolean

    fun snapshot(): List<R>
}

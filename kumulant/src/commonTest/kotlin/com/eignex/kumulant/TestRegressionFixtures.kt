package com.eignex.kumulant

import com.eignex.kumulant.core.RegressionStat
import kotlin.random.Random

// Convergence tolerances depend on this fixture's sample count and noise scale.
internal fun fitLine(stat: RegressionStat<*>, slope: DoubleArray, intercept: Double, n: Int = 4000, seed: Long = 42L) {
    val rng = Random(seed)
    repeat(n) {
        val x = DoubleArray(slope.size) { rng.nextDouble() * 2.0 - 1.0 }
        var y = intercept
        for (i in slope.indices) y += slope[i] * x[i]
        y += rng.nextDouble() * 0.02 - 0.01
        stat.update(x, y, 1.0)
    }
}

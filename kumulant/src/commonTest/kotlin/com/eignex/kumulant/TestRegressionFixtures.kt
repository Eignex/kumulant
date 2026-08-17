package com.eignex.kumulant

import com.eignex.kumulant.core.RegressionStat
import kotlin.random.Random

/**
 * Trains [stat] on a noisy linear ground truth, so a test can then assert the coefficients came back.
 *
 * The convergence tolerances downstream are tuned to these numbers: `n = 4000` draws, features uniform on
 * `[-1, 1]`, noise uniform on `[-0.01, 0.01]`, and a fixed [seed] so the family cannot flake.
 */
internal fun fitLine(stat: RegressionStat<*>, slope: DoubleArray, intercept: Double, n: Int = 4000, seed: Long = 42L) {
    val rng = Random(seed)
    repeat(n) {
        val x = DoubleArray(slope.size) { rng.nextDouble() * 2.0 - 1.0 }
        var y = intercept
        for (i in slope.indices) y += slope[i] * x[i]
        y += rng.nextDouble() * 0.02 - 0.01 // small noise
        stat.update(x, y, 1.0)
    }
}

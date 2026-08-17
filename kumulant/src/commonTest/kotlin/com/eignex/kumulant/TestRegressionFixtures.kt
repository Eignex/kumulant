package com.eignex.kumulant

import com.eignex.kumulant.core.RegressionStat
import kotlin.random.Random

/**
 * Trains [stat] on a noisy linear ground truth, so a test can then assert the coefficients came back.
 *
 * Four test files carried this identical body, and the duplication was not harmless: the convergence
 * assertions downstream of it are tuned to *these* numbers. `n = 4000` draws, features uniform on
 * `[-1, 1]`, and noise uniform on `[-0.01, 0.01]` are what make a tolerance of `0.05` the right call
 * rather than a coin flip, and a fixed [seed] is what stops the whole family flaking one run in fifty.
 * With four copies, tightening the noise in one file silently changed what its tolerance meant while the
 * other three kept the old bargain.
 *
 * Typed against [RegressionStat] rather than a concrete stat because every caller only needs `update`.
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

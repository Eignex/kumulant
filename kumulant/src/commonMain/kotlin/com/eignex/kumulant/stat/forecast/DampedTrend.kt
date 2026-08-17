package com.eignex.kumulant.stat.forecast

import kotlin.math.pow

/**
 * The damped-trend multiplier `phi + phi^2 + ... + phi^steps`, in closed form.
 *
 * Closed form rather than a loop so `forecast(Int.MAX_VALUE)` is O(1). At `phi == 1.0` the series
 * degenerates to `steps` and the closed form would divide by zero, hence the special case.
 */
internal fun dampedTrendSum(phi: Double, steps: Int): Double =
    if (phi == 1.0) steps.toDouble() else phi * (1.0 - phi.pow(steps.toDouble())) / (1.0 - phi)

/** Guards the damping factor. Zero is excluded: it would erase the trend term rather than damp it. */
internal fun requirePhi(phi: Double) {
    require(phi > 0.0 && phi <= 1.0) { "phi must be in (0, 1], got $phi" }
}

/** Guards a forecast horizon. Zero is allowed and means "the current level". */
internal fun requireForecastSteps(steps: Int) {
    require(steps >= 0) { "forecast steps must be >= 0, got $steps" }
}

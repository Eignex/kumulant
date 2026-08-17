package com.eignex.kumulant.stat.forecast

import kotlin.math.pow

/**
 * The damped-trend multiplier `phi + phi^2 + ... + phi^steps`, in closed form.
 *
 * [HoltStat] and [SeasonalSmoothingStat] each carried this, along with the `phi == 1.0` special case
 * where the series degenerates to `steps` and the closed form would divide by zero.
 *
 * The closed form is not an optimisation detail to be rediscovered: `HoltStat` records that the loop it
 * replaced made `forecast(Int.MAX_VALUE)` stall for seconds. A second copy is a second place for that to
 * come back, and the two had already drifted in shape - Holt returned early on `phi == 1.0` while the
 * seasonal version folded the case into the sum - which is how a fix lands on one and not the other.
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

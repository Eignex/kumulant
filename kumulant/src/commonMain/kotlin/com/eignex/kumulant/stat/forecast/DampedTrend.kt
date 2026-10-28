package com.eignex.kumulant.stat.forecast

import kotlin.math.pow

// Closed-form sum phi + phi^2 + ... + phi^steps keeps large forecast horizons O(1).
internal fun dampedTrendSum(phi: Double, steps: Int): Double =
    if (phi == 1.0) steps.toDouble() else phi * (1.0 - phi.pow(steps.toDouble())) / (1.0 - phi)

internal fun requirePhi(phi: Double) {
    require(phi > 0.0 && phi <= 1.0) { "phi must be in (0, 1], got $phi" }
}

internal fun requireForecastSteps(steps: Int) {
    require(steps >= 0) { "forecast steps must be >= 0, got $steps" }
}

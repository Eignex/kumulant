package com.eignex.kumulant.schema.ops

import com.eignex.kumulant.math.PROBABILITY_FLOOR
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.Y
import com.eignex.kumulant.schema.expr.abs
import com.eignex.kumulant.schema.expr.ln
import com.eignex.kumulant.schema.expr.max
import com.eignex.kumulant.schema.expr.min
import com.eignex.kumulant.schema.expr.minus
import com.eignex.kumulant.schema.expr.plus
import com.eignex.kumulant.schema.expr.times
import com.eignex.kumulant.schema.expr.unaryMinus
import com.eignex.kumulant.schema.spec.Mean
import com.eignex.kumulant.schema.spec.PairedStatSpec
import com.eignex.kumulant.stat.summary.WeightedMeanResult

// Each loss is the weighted mean of a per-pair score over paired `(prediction, truth)` input,
// so it merges, windows and serializes exactly as `Mean.foldPaired(score)` does.

/**
 * Mean squared error `(prediction - truth)^2`. For probability forecasts against outcomes
 * in `{0, 1}` this is the Brier score.
 */
fun mseLoss(): PairedStatSpec<WeightedMeanResult> = Mean.foldPaired((X - Y) * (X - Y))

/** Mean absolute error `|prediction - truth|`; less sensitive to outliers than [mseLoss]. */
fun maeLoss(): PairedStatSpec<WeightedMeanResult> = Mean.foldPaired((X - Y).abs())

/**
 * Binary log loss (cross-entropy) `-(y ln(p) + (1 - y) ln(1 - p))` for predicted probability
 * `p` and outcome `y`. `p` is clamped into `[1e-15, 1 - 1e-15]` so a confident wrong
 * prediction scores a large finite loss rather than infinity.
 */
fun logLoss(): PairedStatSpec<WeightedMeanResult> {
    val p = X.max(PROBABILITY_FLOOR).min(1.0 - PROBABILITY_FLOOR)
    return Mean.foldPaired(-(Y * p.ln() + (1.0 - Y) * (1.0 - p).ln()))
}

/**
 * Pinball (quantile) loss at level [tau]: `max(tau (y - yhat), (tau - 1)(y - yhat))`, which is
 * half the absolute error at `tau = 0.5`. Scores a model that predicts the `tau` quantile.
 */
fun pinballLoss(tau: Double): PairedStatSpec<WeightedMeanResult> {
    require(tau in 0.0..1.0) { "tau must be in [0, 1]; got $tau" }
    val residual = Y - X
    return Mean.foldPaired((tau * residual).max((tau - 1.0) * residual))
}

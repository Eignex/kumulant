package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.expr.Center
import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.expr.High
import com.eignex.kumulant.schema.expr.IfExpr
import com.eignex.kumulant.schema.expr.Low
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.Scale
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.div
import com.eignex.kumulant.schema.expr.gt
import com.eignex.kumulant.schema.expr.minus
import com.eignex.kumulant.schema.expr.plus
import com.eignex.kumulant.schema.expr.times
import com.eignex.kumulant.stat.summary.RangeStat
import com.eignex.kumulant.stat.summary.VarianceStat

// Standard streaming feature scalers built on top of `withFeedback`. Each scaler couples the
// inner stat with a small running-statistics primary so the projected input is rescaled per
// update against the snapshot just observed.
//
// All scalers degrade gracefully during the warm-up phase (variance/range still zero): they
// emit the configured neutral value rather than NaN.

/** AST: `(X - center) / scale` guarded by `Scale > 0`, emitting `0` while the variance is
 *  still degenerate. */
internal val standardScalerProjection: ScalarExpr =
    IfExpr(Scale gt 0.0, (X - Center) / Scale, Const(0.0))

internal fun minMaxProjection(targetLow: Double, targetHigh: Double): ScalarExpr {
    val span = High - Low
    val normalized = (X - Low) / span
    val scaled = normalized * (targetHigh - targetLow) + targetLow
    return IfExpr(span gt 0.0, scaled, Const(targetLow))
}

/**
 * Z-score the input against the inner stat's own running mean and standard deviation, then
 * forward the standardized value to this stat. Wraps a hidden [VarianceStat] primary; while
 * the variance is still zero (the very first update) the scaler emits `0`.
 */
internal fun <R : Result> SeriesStat<R>.standardScaler(concurrency: Concurrency = this.concurrency): SeriesStat<R> =
    withFeedback(VarianceStat(concurrency), standardScalerProjection)

/**
 * Min-max scale the input against the running observed range, then forward the mapped value
 * to this stat. With the defaults the output lands in `[0, 1]`; use `targetLow = -1.0,
 * targetHigh = 1.0` for a `[-1, 1]` mapping. While the range is still degenerate (first
 * update or constant stream) the scaler emits [targetLow].
 */
internal fun <R : Result> SeriesStat<R>.minMaxScaler(
    targetLow: Double = 0.0,
    targetHigh: Double = 1.0,
    concurrency: Concurrency = this.concurrency,
): SeriesStat<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    return withFeedback(RangeStat(concurrency), minMaxProjection(targetLow, targetHigh))
}

/**
 * Element-wise standard scaler over a [dimensions]-dimensional vector input. Each
 * coordinate carries its own [VarianceStat] primary; the inner vector stat sees a
 * per-coordinate z-scored vector. Each coordinate degrades to `0` while its
 * variance is still zero.
 */
@Suppress("UNCHECKED_CAST")
internal fun <I : Result> VectorStat<I>.standardScaleFeatures(
    dimensions: Int,
    concurrency: Concurrency = this.concurrency,
): VectorStat<I> {
    val primary = VectorizedStat(dimensions, VarianceStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, standardScalerProjection)
}

/**
 * Element-wise min-max scaler over a [dimensions]-dimensional vector input. Each
 * coordinate carries its own [RangeStat] primary; the inner vector stat sees a
 * per-coordinate `[targetLow, targetHigh]`-mapped vector. Each coordinate degrades
 * to [targetLow] while its range is still degenerate.
 */
@Suppress("UNCHECKED_CAST")
internal fun <I : Result> VectorStat<I>.minMaxScaleFeatures(
    dimensions: Int,
    targetLow: Double = 0.0,
    targetHigh: Double = 1.0,
    concurrency: Concurrency = this.concurrency,
): VectorStat<I> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    val primary = VectorizedStat(dimensions, RangeStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, minMaxProjection(targetLow, targetHigh))
}

/**
 * Element-wise standard scaler over the regression's feature vector. Each coordinate
 * carries its own [VarianceStat] primary; the inner regressor sees a per-feature
 * z-scored vector. `y` and `weight` pass through unchanged.
 */
@Suppress("UNCHECKED_CAST")
internal fun <R : Result> RegressionStat<R>.standardScaleFeatures(
    concurrency: Concurrency = this.concurrency,
): RegressionStat<R> {
    val primary = VectorizedStat(featureSize, VarianceStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, standardScalerProjection)
}

/**
 * Element-wise min-max scaler over the regression's feature vector. Each coordinate
 * carries its own [RangeStat] primary; the inner regressor sees a per-feature
 * rescaled vector. `y` and `weight` pass through unchanged.
 */
@Suppress("UNCHECKED_CAST")
internal fun <R : Result> RegressionStat<R>.minMaxScaleFeatures(
    targetLow: Double = 0.0,
    targetHigh: Double = 1.0,
    concurrency: Concurrency = this.concurrency,
): RegressionStat<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    val primary = VectorizedStat(featureSize, RangeStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, minMaxProjection(targetLow, targetHigh))
}

/**
 * Z-score both axes of a paired stat against per-axis [VarianceStat] primaries, then
 * forward the standardized `(x', y')` to the inner. Each axis degrades to `0` while
 * its variance is still zero.
 */
internal fun <R : Result> PairedStat<R>.standardScaler(concurrency: Concurrency = this.concurrency): PairedStat<R> =
    withFeedback(VarianceStat(concurrency), VarianceStat(concurrency), standardScalerProjection)

/**
 * Min-max scale both axes of a paired stat against per-axis [RangeStat] primaries into
 * `[targetLow, targetHigh]`. Each axis degrades to [targetLow] while its range is still
 * degenerate.
 */
internal fun <R : Result> PairedStat<R>.minMaxScaler(
    targetLow: Double = 0.0,
    targetHigh: Double = 1.0,
    concurrency: Concurrency = this.concurrency,
): PairedStat<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    return withFeedback(
        RangeStat(concurrency),
        RangeStat(concurrency),
        minMaxProjection(targetLow, targetHigh),
    )
}

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
import com.eignex.kumulant.stream.DEFAULT_TARGET_HIGH
import com.eignex.kumulant.stream.DEFAULT_TARGET_LOW

// Scalers project each input against the just-updated primary snapshot.
// Zero variance or range maps to the configured neutral value.

// Zero variance maps to the neutral value 0.
internal val standardScalerProjection: ScalarExpr =
    IfExpr(Scale gt 0.0, (X - Center) / Scale, Const(0.0))

internal fun minMaxProjection(targetLow: Double, targetHigh: Double): ScalarExpr {
    val span = High - Low
    val normalized = (X - Low) / span
    val scaled = normalized * (targetHigh - targetLow) + targetLow
    return IfExpr(span gt 0.0, scaled, Const(targetLow))
}

internal fun <R : Result> SeriesStat<R>.standardScaler(concurrency: Concurrency = this.concurrency): SeriesStat<R> =
    withFeedback(VarianceStat(concurrency), standardScalerProjection)

// A degenerate observed range maps to targetLow.
internal fun <R : Result> SeriesStat<R>.minMaxScaler(
    targetLow: Double = DEFAULT_TARGET_LOW,
    targetHigh: Double = DEFAULT_TARGET_HIGH,
    concurrency: Concurrency = this.concurrency,
): SeriesStat<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    return withFeedback(RangeStat(concurrency), minMaxProjection(targetLow, targetHigh))
}

@Suppress("UNCHECKED_CAST")
internal fun <I : Result> VectorStat<I>.standardScaleFeatures(
    dimensions: Int,
    concurrency: Concurrency = this.concurrency,
): VectorStat<I> {
    val primary = VectorizedStat(dimensions, VarianceStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, standardScalerProjection)
}

@Suppress("UNCHECKED_CAST")
internal fun <I : Result> VectorStat<I>.minMaxScaleFeatures(
    dimensions: Int,
    targetLow: Double = DEFAULT_TARGET_LOW,
    targetHigh: Double = DEFAULT_TARGET_HIGH,
    concurrency: Concurrency = this.concurrency,
): VectorStat<I> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    val primary = VectorizedStat(dimensions, RangeStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, minMaxProjection(targetLow, targetHigh))
}

@Suppress("UNCHECKED_CAST")
internal fun <R : Result> RegressionStat<R>.standardScaleFeatures(
    concurrency: Concurrency = this.concurrency,
): RegressionStat<R> {
    val primary = VectorizedStat(featureSize, VarianceStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, standardScalerProjection)
}

@Suppress("UNCHECKED_CAST")
internal fun <R : Result> RegressionStat<R>.minMaxScaleFeatures(
    targetLow: Double = DEFAULT_TARGET_LOW,
    targetHigh: Double = DEFAULT_TARGET_HIGH,
    concurrency: Concurrency = this.concurrency,
): RegressionStat<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    val primary = VectorizedStat(featureSize, RangeStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, minMaxProjection(targetLow, targetHigh))
}

internal fun <R : Result> PairedStat<R>.standardScaler(concurrency: Concurrency = this.concurrency): PairedStat<R> =
    withFeedback(VarianceStat(concurrency), VarianceStat(concurrency), standardScalerProjection)

internal fun <R : Result> PairedStat<R>.minMaxScaler(
    targetLow: Double = DEFAULT_TARGET_LOW,
    targetHigh: Double = DEFAULT_TARGET_HIGH,
    concurrency: Concurrency = this.concurrency,
): PairedStat<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    return withFeedback(
        RangeStat(concurrency),
        RangeStat(concurrency),
        minMaxProjection(targetLow, targetHigh),
    )
}

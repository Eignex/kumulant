package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.expr.MinMax
import com.eignex.kumulant.schema.expr.Standardize
import com.eignex.kumulant.stat.summary.RangeStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stream.DEFAULT_TARGET_HIGH
import com.eignex.kumulant.stream.DEFAULT_TARGET_LOW

// Regression scalers project each feature against the just-updated per-coordinate primary,
// sized from the regressor's featureSize at build time. Zero variance or range maps to the
// neutral value.

@Suppress("UNCHECKED_CAST")
internal fun <R : Result> RegressionStat<R>.standardScaleFeatures(
    concurrency: Concurrency = this.concurrency,
): RegressionStat<R> {
    val primary = VectorizedStat(featureSize, VarianceStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, Standardize)
}

@Suppress("UNCHECKED_CAST")
internal fun <R : Result> RegressionStat<R>.minMaxScaleFeatures(
    targetLow: Double = DEFAULT_TARGET_LOW,
    targetHigh: Double = DEFAULT_TARGET_HIGH,
    concurrency: Concurrency = this.concurrency,
): RegressionStat<R> {
    val primary = VectorizedStat(featureSize, RangeStat(concurrency))
        as VectorStat<ResultList<Result>>
    return withFeedback(primary, MinMax(targetLow, targetHigh))
}

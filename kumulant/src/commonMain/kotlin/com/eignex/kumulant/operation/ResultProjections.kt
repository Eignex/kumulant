package com.eignex.kumulant.operation

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.schema.spec.ResultProjection
import com.eignex.kumulant.stat.calibration.PlattCalibratorResult
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.glm.Link
import com.eignex.kumulant.stat.regression.glm.Penalty
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.score.AccuracyResult
import com.eignex.kumulant.stat.summary.MadResult
import com.eignex.kumulant.stat.summary.VarianceResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.medianAbsoluteDeviation

internal fun SeriesStat<Result>.project(projection: ResultProjection): SeriesStat<Result> {
    val baseline = read(0L)
    projection.forward(baseline)
    return mapResult(projection::forward) { projection.reverse(it, baseline) }
}

internal fun PairedStat<Result>.project(projection: ResultProjection): PairedStat<Result> {
    val baseline = read(0L)
    projection.forward(baseline)
    return mapResult(projection::forward) { projection.reverse(it, baseline) }
}

private inline fun <reified R : Result> ResultProjection.input(result: Result): R {
    require(result is R) { "$this requires ${R::class.simpleName}; got ${result::class.simpleName}" }
    return result
}

private fun ResultProjection.forward(result: Result): Result = when (this) {
    ResultProjection.Mad -> input<TDigestResult>(result).medianAbsoluteDeviation()

    ResultProjection.Covariance -> input<UnivariateRegressionResult>(result).let {
        CovarianceResult(
            it.totalWeights,
            it.x.mean,
            it.y.mean,
            it.sxy,
            it.x.variance * it.totalWeights,
            it.y.variance * it.totalWeights,
        )
    }

    ResultProjection.PlattCalibration -> input<StochasticRegressionResult>(result).let {
        require(
            it.link == Link.Logit && it.weights.size == 1,
        ) { "Platt calibration requires a one-feature logit model" }
        PlattCalibratorResult(it.weights[0], it.bias, it.totalWeights, it.step)
    }

    ResultProjection.Accuracy -> input<WeightedMeanResult>(result).let { AccuracyResult(it.mean, it.totalWeights) }
}

private fun ResultProjection.reverse(result: Result, baseline: Result): Result = when (this) {
    ResultProjection.Mad -> input<MadResult>(result).let {
        it.digest ?: it.toDigest((baseline as TDigestResult).compression)
    }

    ResultProjection.Covariance -> input<CovarianceResult>(result).toRegression()

    ResultProjection.PlattCalibration -> input<PlattCalibratorResult>(result).let {
        StochasticRegressionResult(
            weights = DenseVector.of(doubleArrayOf(it.slope)),
            bias = it.intercept,
            totalWeights = it.totalWeights,
            step = it.step,
            link = Link.Logit,
            sse = 0.0,
        )
    }

    ResultProjection.Accuracy -> input<AccuracyResult>(result).let { WeightedMeanResult(it.totalWeights, it.accuracy) }
}

private fun MadResult.toDigest(compression: Double): TDigestResult {
    val deviation = if (mad.isNaN()) 0.0 else mad
    val means = if (median.isNaN()) {
        doubleArrayOf()
    } else {
        doubleArrayOf(
            median - deviation,
            median + deviation,
        ).sortedArray()
    }
    return TDigestResult(
        probabilities = doubleArrayOf(),
        quantiles = doubleArrayOf(),
        means = means,
        weights = DoubleArray(means.size) { 0.5 },
        totalWeight = if (means.isEmpty()) 0.0 else 1.0,
        compression = compression,
    )
}

private fun CovarianceResult.toRegression(): UnivariateRegressionResult {
    val slope = if (sxx > 0.0) sxy / sxx else 0.0
    return UnivariateRegressionResult(
        penalty = Penalty.None,
        totalWeights = totalWeights,
        slope = slope,
        intercept = meanY - slope * meanX,
        sse = (syy - slope * sxy).coerceAtLeast(0.0),
        sxy = sxy,
        x = VarianceResult(meanX, if (totalWeights > 0.0) sxx / totalWeights else 0.0),
        y = VarianceResult(meanY, if (totalWeights > 0.0) syy / totalWeights else 0.0),
    )
}

package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.quantile.LinearHistogramStat
import com.eignex.kumulant.stat.quantile.SparseHistogramResult

/**
 * Probability Integral Transform histogram: bins `F(y)` (the forecast CDF
 * evaluated at the observed truth) into [numBins] equal-width buckets across
 * `[0, 1]`. A uniform empirical distribution indicates a well-calibrated
 * forecaster; concentrated mass indicates miscalibration.
 *
 * Caller computes `pit = forecast.cdf(y)` upstream and feeds it as the value.
 * Backed by [LinearHistogramStat] with bounds pinned to `[0, 1]`.
 */
fun pitHistogram(numBins: Int, concurrency: Concurrency = Concurrency.None): SeriesStat<SparseHistogramResult> =
    LinearHistogramStat(0.0, 1.0, numBins, concurrency)

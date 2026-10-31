package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.quantile.BucketHistogramStat
import com.eignex.kumulant.stat.quantile.SparseHistogramResult

/**
 * Probability Integral Transform histogram: bins `F(y)` (the forecast CDF
 * evaluated at the observed truth) into [numBins] equal-width buckets across
 * `[0, 1]`. A uniform empirical distribution indicates a well-calibrated
 * forecaster; concentrated mass indicates miscalibration.
 *
 * Caller computes `pit = forecast.cdf(y)` upstream and feeds it as the value.
 * Backed by [BucketHistogramStat.linear] over `[0, 1]`; buckets include their upper edge.
 */
fun pitHistogram(numBins: Int, concurrency: Concurrency = Concurrency.None): SeriesStat<SparseHistogramResult> =
    BucketHistogramStat.linear(0.0, 1.0, numBins, concurrency)

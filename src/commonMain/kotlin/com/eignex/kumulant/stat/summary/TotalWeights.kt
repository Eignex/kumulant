package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.defaultConcurrency
import com.eignex.kumulant.operation.withValue

/** Sum of per-update weights — i.e. the effective sample size. */
class TotalWeights(concurrency: Concurrency = defaultConcurrency) :
    SeriesStat<SumResult> by Sum(concurrency).withValue(1.0)

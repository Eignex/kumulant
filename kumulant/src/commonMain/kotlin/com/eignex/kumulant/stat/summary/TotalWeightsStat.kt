package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.operation.withValue

/**
 * Sum of per-update weights — i.e. the effective sample size.
 *
 * # Concurrency
 *
 * Inherits [SumStat]'s single-atomic-add update path — exact under every
 * [Concurrency] level.
 */
class TotalWeightsStat(concurrency: Concurrency = Concurrency.None) :
    SeriesStat<SumResult> by SumStat(concurrency).withValue(1.0)

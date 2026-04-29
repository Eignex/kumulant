package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode

/** Sum of per-update weights — i.e. the effective sample size. */
class TotalWeights(val mode: StreamMode = defaultStreamMode) :
    SeriesStat<SumResult> by Sum(mode).withValue(1.0)

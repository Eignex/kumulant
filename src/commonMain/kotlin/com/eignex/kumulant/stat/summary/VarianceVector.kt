package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.operation.VectorizedStat

/**
 * [VectorStat] producing one [WeightedVarianceResult] per dimension. A
 * discoverable name for the per-dimension variance accumulator that PGBM and
 * similar streaming-ML pipelines reach for.
 */
fun varianceVector(
    dimensions: Int,
    concurrency: Concurrency = Concurrency.None,
): VectorStat<ResultList<WeightedVarianceResult>> =
    VectorizedStat(dimensions, VarianceStat(concurrency), concurrency)

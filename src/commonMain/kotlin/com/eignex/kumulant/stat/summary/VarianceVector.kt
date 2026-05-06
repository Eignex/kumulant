package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.operation.expandedToVector

/**
 * [VectorStat] producing one [WeightedVarianceResult] per dimension.
 *
 * Convenience wrapper around `{ Variance() }.expandedToVector(dimensions)` —
 * gives a discoverable name for the per-dimension variance accumulator that
 * PGBM and similar streaming-ML pipelines reach for.
 */
fun varianceVector(
    dimensions: Int,
    concurrency: Concurrency = Concurrency.None,
): VectorStat<ResultList<WeightedVarianceResult>> = { _: Int -> Variance(concurrency) }.expandedToVector(dimensions)

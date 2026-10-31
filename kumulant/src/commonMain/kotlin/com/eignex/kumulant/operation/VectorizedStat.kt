package com.eignex.kumulant.operation

import com.eignex.koblas.Vector
import com.eignex.koblas.forEachStored
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.core.requireFeatureSize
import kotlin.time.Duration

// skipZeros treats absent coordinates as unobserved, with O(nnz) sparse updates.
// Use it for additive stats; means and variances distinguish absence from zero.
internal class VectorizedStat<R : Result>(
    val dimensions: Int,
    private val template: SeriesStat<R>,
    val skipZeros: Boolean = false,
) : VectorStat<ResultList<R>>,
    WindowsInside<ResultList<R>, VectorStat<ResultList<R>>> {

    private val stats: Array<SeriesStat<R>> =
        Array(dimensions) { template.create(null) }

    override val concurrency: Concurrency get() = template.concurrency

    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        vector.requireFeatureSize(dimensions)
        if (skipZeros) {
            vector.forEachStored { i, v -> stats[i].update(v, timestampNanos, weight) }
        } else {
            for (i in 0 until dimensions) {
                stats[i].update(vector[i], timestampNanos, weight)
            }
        }
    }

    override fun read(timestampNanos: Long): ResultList<R> = ResultList(stats.map { it.read(timestampNanos) })

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<ResultList<R>> =
        VectorizedStat(
            dimensions,
            template.windowed(duration, slices, concurrency),
            skipZeros,
        )

    override fun create(concurrency: Concurrency?): VectorStat<ResultList<R>> =
        VectorizedStat(dimensions, template.create(concurrency), skipZeros)

    override fun validateMerge(values: ResultList<R>) {
        require(values.results.size == dimensions) {
            "merge: results.size=${values.results.size}, expected $dimensions"
        }
        for (i in 0 until dimensions) stats[i].validateMerge(values.results[i])
    }

    override fun merge(values: ResultList<R>, timestampNanos: Long) {
        validateMerge(values)
        for (i in 0 until dimensions) stats[i].merge(values.results[i], timestampNanos)
    }

    override fun reset() {
        for (stat in stats) stat.reset()
    }
}

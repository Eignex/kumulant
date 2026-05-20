package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.math.VectorView

/**
 * Fans each vector observation out to one [SeriesStat] per dimension.
 *
 * Constructed from a single [template] stat replicated across [dimensions]
 * via `template.create(concurrency)`. Produces a [ResultList] with positional
 * entries; incoming vectors must match [dimensions] exactly.
 */
class VectorizedStat<R : Result>(
    val dimensions: Int,
    private val template: SeriesStat<R>,
) : VectorStat<ResultList<R>> {

    private val stats: Array<SeriesStat<R>> =
        Array(dimensions) { template.create(null) }

    override val concurrency: Concurrency get() = template.concurrency

    override fun update(
        vector: VectorView,
        timestampNanos: Long,
        weight: Double,
    ) {
        require(vector.size == dimensions) {
            "Vector size ${vector.size} does not match expected dimensions $dimensions"
        }
        for (i in 0 until dimensions) {
            stats[i].update(vector[i], timestampNanos, weight)
        }
    }

    override fun read(timestampNanos: Long): ResultList<R> =
        ResultList(stats.map { it.read(timestampNanos) })

    override fun create(concurrency: Concurrency?): VectorStat<ResultList<R>> =
        VectorizedStat(dimensions, template.create(concurrency))

    override fun merge(values: ResultList<R>) {
        require(values.results.size == dimensions)
        for (i in 0 until dimensions) {
            stats[i].merge(values.results[i])
        }
    }

    override fun reset() {
        for (stat in stats) stat.reset()
    }
}

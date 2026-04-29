package com.eignex.kumulant.operation

import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat

/** Expand a template factory into a [VectorStat] with one [SeriesStat] per of the [dimensions] slots. */
fun <R : Result> ((Int) -> SeriesStat<R>).expandedToVector(
    dimensions: Int
): VectorStat<ResultList<R>> {
    return VectorizedStat(dimensions, this)
}

/**
 * Fans each vector observation out to one [SeriesStat] per dimension.
 *
 * Produces a [ResultList] with positional entries. Incoming vectors must match the
 * declared [dimensions] exactly.
 */
class VectorizedStat<R : Result>(
    val dimensions: Int,
    val template: (index: Int) -> SeriesStat<R>,
    val mode: StreamMode? = null
) : VectorStat<ResultList<R>> {

    private val stats: Array<SeriesStat<R>> =
        Array(dimensions) { i -> template(i) }

    override fun update(
        vector: DoubleArray,
        timestampNanos: Long,
        weight: Double
    ) {
        require(vector.size == dimensions) {
            "Vector size ${vector.size} does not match expected dimensions $dimensions"
        }

        for (i in 0 until dimensions) {
            stats[i].update(vector[i], timestampNanos, weight)
        }
    }

    override fun read(timestampNanos: Long): ResultList<R> {
        return ResultList(stats.map { it.read(timestampNanos) })
    }

    override fun create(mode: StreamMode?): VectorStat<ResultList<R>> =
        VectorizedStat(dimensions, template, mode ?: this.mode)

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

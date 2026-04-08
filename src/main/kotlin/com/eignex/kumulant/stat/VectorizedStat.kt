package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat


fun <R : Result> ((Int) -> SeriesStat<R>).expandedToVector(
    dimensions: Int
): VectorStat<ResultList<R>> {
    return VectorizedStat(dimensions, this)
}
class VectorizedStat<R : Result>(
    val dimensions: Int,
    val template: (index: Int) -> SeriesStat<R>,
    override val name: String? = null,
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
        return ResultList(stats.map { it.read(timestampNanos) }, name)
    }

    override fun copy(
        mode: StreamMode?,
        name: String?
    ): Stat<ResultList<R>> {
        return VectorizedStat(dimensions, template, name ?: this.name, mode ?: this.mode)
    }

    override fun merge(values: ResultList<R>) {
        require(values.results.size == dimensions)
        for (i in 0 until dimensions) {
            @Suppress("UNCHECKED_CAST")
            stats[i].merge(values.results[i] as R)
        }
    }

    override fun reset() {
        for (stat in stats) stat.reset()
    }
}

package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.math.VectorView

/** Adapter implementing [atX]: drives a [SeriesStat] from the x coordinate of a pair. */
internal class AtXStat<R : Result>(private val delegate: SeriesStat<R>) :
    PairedStat<R>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> = AtXStat(delegate.create(concurrency))
}

/** Adapter implementing [atY]: drives a [SeriesStat] from the y coordinate of a pair. */
internal class AtYStat<R : Result>(private val delegate: SeriesStat<R>) :
    PairedStat<R>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(y, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> = AtYStat(delegate.create(concurrency))
}

/** Adapter implementing [atIndex]: drives a [SeriesStat] from one slot of a vector. */
internal class AtIndexStat<R : Result>(private val delegate: SeriesStat<R>, private val index: Int) :
    VectorStat<R>,
    Stat<R> by delegate {
    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        delegate.update(vector[index], timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<R> = AtIndexStat(delegate.create(concurrency), index)
}

/** Adapter implementing [atIndices]: drives a [PairedStat] from two slots of a vector. */
internal class AtIndicesStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val indexX: Int,
    private val indexY: Int,
) : VectorStat<R>,
    Stat<R> by delegate {
    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        delegate.update(vector[indexX], vector[indexY], timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        AtIndicesStat(delegate.create(concurrency), indexX, indexY)
}

/** Adapt a [SeriesStat] to consume only the x coordinate of a paired input. */
fun <R : Result> SeriesStat<R>.atX(): PairedStat<R> = AtXStat(this)

/** Adapt a [SeriesStat] to consume only the y coordinate of a paired input. */
fun <R : Result> SeriesStat<R>.atY(): PairedStat<R> = AtYStat(this)

/** Adapt a [SeriesStat] to consume `vector[index]` of each incoming vector. */
fun <R : Result> SeriesStat<R>.atIndex(index: Int): VectorStat<R> = AtIndexStat(this, index)

/** Adapt a [PairedStat] to consume (`vector[indexX]`, `vector[indexY]`) of each vector. */
fun <R : Result> PairedStat<R>.atIndices(indexX: Int, indexY: Int): VectorStat<R> = AtIndicesStat(this, indexX, indexY)

package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

class AtXStat<R : Result>(
    private val delegate: SeriesStat<R>
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): PairedStat<R> {
        return AtXStat(delegate.create(mode))
    }
}

class AtYStat<R : Result>(
    private val delegate: SeriesStat<R>
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(y, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): PairedStat<R> {
        return AtYStat(delegate.create(mode))
    }
}

class AtIndexStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val index: Int
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(vector[index], timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): VectorStat<R> {
        return AtIndexStat(delegate.create(mode), index)
    }
}

class AtIndicesStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val indexX: Int,
    private val indexY: Int
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(vector[indexX], vector[indexY], timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): VectorStat<R> {
        return AtIndicesStat(delegate.create(mode), indexX, indexY)
    }
}

fun <R : Result> SeriesStat<R>.atX(): PairedStat<R> = AtXStat(this)

fun <R : Result> SeriesStat<R>.atY(): PairedStat<R> = AtYStat(this)

fun <R : Result> SeriesStat<R>.atIndex(index: Int): VectorStat<R> = AtIndexStat(this, index)

fun <R : Result> PairedStat<R>.atIndices(indexX: Int, indexY: Int): VectorStat<R> = AtIndicesStat(this, indexX, indexY)

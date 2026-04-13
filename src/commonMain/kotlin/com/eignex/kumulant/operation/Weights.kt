package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

fun <R : Result> SeriesStat<R>.withWeight(weight: Double): SeriesStat<R> = WithWeightStat(this, weight)

fun <R : Result> PairedStat<R>.withWeight(weight: Double): PairedStat<R> = WithWeightPairedStat(this, weight)

fun <R : Result> VectorStat<R>.withWeight(weight: Double): VectorStat<R> = WithWeightVectorStat(this, weight)

class WithWeightStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val weight: Double
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, this.weight)
    }

    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithWeightStat(delegate.create(mode), weight)
    }
}

class WithWeightPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val weight: Double
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, y, timestampNanos, this.weight)
    }

    override fun create(mode: StreamMode?): PairedStat<R> {
        return WithWeightPairedStat(delegate.create(mode), weight)
    }
}

class WithWeightVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val weight: Double
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(vector, timestampNanos, this.weight)
    }

    override fun create(mode: StreamMode?): VectorStat<R> {
        return WithWeightVectorStat(delegate.create(mode), weight)
    }
}

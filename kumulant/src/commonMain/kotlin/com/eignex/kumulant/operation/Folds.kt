package com.eignex.kumulant.operation

import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import kotlin.time.Duration

internal class FoldVectorStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: (Vector) -> Double,
) : VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        delegate.update(transform(vector), timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        FoldVectorStat(delegate.windowed(duration, slices, concurrency), transform)

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        FoldVectorStat(delegate.create(concurrency), transform)
}

internal class FoldPairedStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: (Double, Double) -> Double,
) : PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform(x, y), timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        FoldPairedStat(delegate.windowed(duration, slices, concurrency), transform)

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        FoldPairedStat(delegate.create(concurrency), transform)
}

internal class FoldVectorPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val foldX: (Vector) -> Double,
    private val foldY: (Vector) -> Double,
) : VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        delegate.update(foldX(vector), foldY(vector), timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        FoldVectorPairedStat(delegate.windowed(duration, slices, concurrency), foldX, foldY)

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        FoldVectorPairedStat(delegate.create(concurrency), foldX, foldY)
}

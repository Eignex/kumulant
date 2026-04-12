package com.eignex.kumulant.concurrent

import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

fun <R : Result> SeriesStat<R>.locked() = LockedSeriesStat(this)
fun <R : Result> PairedStat<R>.locked() = LockedPairedStat(this)
fun <R : Result> VectorStat<R>.locked() = LockedVectorStat(this)

class LockedSeriesStat<R : Result>(private val delegate: SeriesStat<R>) :
    SeriesStat<R> by delegate {
    private val lock = ReentrantReadWriteLock()

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        lock.write { delegate.update(value, timestampNanos, weight) }
    }

    override fun merge(values: R) {
        lock.write { delegate.merge(values) }
    }

    override fun reset() {
        lock.write { delegate.reset() }
    }

    override fun read(timestampNanos: Long): R {
        return lock.read { delegate.read(timestampNanos) }
    }

    override fun create(mode: StreamMode?): SeriesStat<R> =
        LockedSeriesStat(delegate.create(mode))
}

class LockedPairedStat<R : Result>(private val delegate: PairedStat<R>) :
    PairedStat<R> by delegate {
    private val lock = ReentrantReadWriteLock()

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        lock.write { delegate.update(x, y, timestampNanos, weight) }
    }

    override fun merge(values: R) {
        lock.write { delegate.merge(values) }
    }

    override fun reset() {
        lock.write { delegate.reset() }
    }

    override fun read(timestampNanos: Long): R {
        return lock.read { delegate.read(timestampNanos) }
    }

    override fun create(mode: StreamMode?): PairedStat<R> =
        LockedPairedStat(delegate.create(mode))
}

class LockedVectorStat<R : Result>(private val delegate: VectorStat<R>) :
    VectorStat<R> by delegate {
    private val lock = ReentrantReadWriteLock()

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        lock.write { delegate.update(vector, timestampNanos, weight) }
    }

    override fun merge(values: R) {
        lock.write { delegate.merge(values) }
    }

    override fun reset() {
        lock.write { delegate.reset() }
    }

    override fun read(timestampNanos: Long): R {
        return lock.read { delegate.read(timestampNanos) }
    }

    override fun create(mode: StreamMode?): VectorStat<R> =
        LockedVectorStat(delegate.create(mode))
}

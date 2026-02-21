package com.eignex.katom.core

import com.eignex.katom.concurrent.StreamMode

// --- Primitive Functional Interfaces to prevent Autoboxing ---

fun interface VectorTransform {
    fun apply(vector: DoubleArray): Double
}

fun interface PairedTransform {
    fun apply(x: Double, y: Double): Double
}

fun interface DoubleTransform {
    fun apply(value: Double): Double
}

fun interface DoublePredicate {
    fun test(value: Double): Boolean
}

fun interface PairedPredicate {
    fun test(x: Double, y: Double): Boolean
}

fun interface VectorPredicate {
    fun test(vector: DoubleArray): Boolean
}

// --- Concrete Decorator Classes for JIT Devirtualization ---

class MapFromVectorStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: VectorTransform
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(vector), timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): VectorStat<R> {
        return MapFromVectorStat(delegate.copy(name, mode), transform)
    }
}

class MapFromPairedStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: PairedTransform
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(x, y), timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): PairedStat<R> {
        return MapFromPairedStat(delegate.copy(name, mode), transform)
    }
}

class MapSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: DoubleTransform
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(value), timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): SeriesStat<R> {
        return MapSeriesStat(delegate.copy(name, mode), transform)
    }
}

class OnXStat<R : Result>(
    private val delegate: SeriesStat<R>
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): PairedStat<R> {
        return OnXStat(delegate.copy(name, mode))
    }
}

class OnYStat<R : Result>(
    private val delegate: SeriesStat<R>
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(y, timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): PairedStat<R> {
        return OnYStat(delegate.copy(name, mode))
    }
}

class AtIndexStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val index: Int
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(vector[index], timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): VectorStat<R> {
        return AtIndexStat(delegate.copy(name, mode), index)
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
    override fun copy(name: String?, mode: StreamMode?): VectorStat<R> {
        return AtIndicesStat(delegate.copy(name, mode) as PairedStat<R>, indexX, indexY)
    }
}

class WithTimeAsXStat<R : Result>(
    private val delegate: PairedStat<R>
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = timestampNanos / 1e9, y = value, timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): SeriesStat<R> {
        return WithTimeAsXStat(delegate.copy(name, mode) as PairedStat<R>)
    }
}

class WithTimeAsYStat<R : Result>(
    private val delegate: PairedStat<R>
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = value, y = timestampNanos / 1e9, timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): SeriesStat<R> {
        return WithTimeAsYStat(delegate.copy(name, mode) as PairedStat<R>)
    }
}

class WithFixedXStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val fixedX: Double
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = fixedX, y = value, timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): SeriesStat<R> {
        return WithFixedXStat(delegate.copy(name, mode) as PairedStat<R>, fixedX)
    }
}

class WithFixedYStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val fixedY: Double
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = value, y = fixedY, timestampNanos, weight)
    }
    override fun copy(name: String?, mode: StreamMode?): SeriesStat<R> {
        return WithFixedYStat(delegate.copy(name, mode) as PairedStat<R>, fixedY)
    }
}

class FilterSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val predicate: DoublePredicate
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (predicate.test(value)) {
            delegate.update(value, timestampNanos, weight)
        }
    }
    override fun copy(name: String?, mode: StreamMode?): SeriesStat<R> {
        return FilterSeriesStat(delegate.copy(name, mode), predicate)
    }
}

class FilterPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val predicate: PairedPredicate
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (predicate.test(x, y)) {
            delegate.update(x, y, timestampNanos, weight)
        }
    }
    override fun copy(name: String?, mode: StreamMode?): PairedStat<R> {
        return FilterPairedStat(delegate.copy(name, mode) as PairedStat<R>, predicate)
    }
}

class FilterVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val predicate: VectorPredicate
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        if (predicate.test(vector)) {
            delegate.update(vector, timestampNanos, weight)
        }
    }
    override fun copy(name: String?, mode: StreamMode?): VectorStat<R> {
        return FilterVectorStat(delegate.copy(name, mode) as VectorStat<R>, predicate)
    }
}

// --- Fluent Extension Factories ---

fun <R : Result> SeriesStat<R>.mapFromVector(transform: VectorTransform): VectorStat<R> = MapFromVectorStat(this, transform)
fun <R : Result> SeriesStat<R>.mapFromPaired(transform: PairedTransform): PairedStat<R> = MapFromPairedStat(this, transform)
fun <R : Result> SeriesStat<R>.mapSeries(transform: DoubleTransform): SeriesStat<R> = MapSeriesStat(this, transform)

fun <R : Result> SeriesStat<R>.onX(): PairedStat<R> = OnXStat(this)
fun <R : Result> SeriesStat<R>.onY(): PairedStat<R> = OnYStat(this)

fun <R : Result> SeriesStat<R>.atIndex(index: Int): VectorStat<R> = AtIndexStat(this, index)
fun <R : Result> PairedStat<R>.atIndices(indexX: Int, indexY: Int): VectorStat<R> = AtIndicesStat(this, indexX, indexY)

fun <R : Result> PairedStat<R>.withTimeAsX(): SeriesStat<R> = WithTimeAsXStat(this)
fun <R : Result> PairedStat<R>.withTimeAsY(): SeriesStat<R> = WithTimeAsYStat(this)
fun <R : Result> PairedStat<R>.withFixedX(fixedX: Double): SeriesStat<R> = WithFixedXStat(this, fixedX)
fun <R : Result> PairedStat<R>.withFixedY(fixedY: Double): SeriesStat<R> = WithFixedYStat(this, fixedY)

fun <R : Result> SeriesStat<R>.filter(predicate: DoublePredicate): SeriesStat<R> = FilterSeriesStat(this, predicate)
fun <R : Result> PairedStat<R>.filter(predicate: PairedPredicate): PairedStat<R> = FilterPairedStat(this, predicate)
fun <R : Result> VectorStat<R>.filter(predicate: VectorPredicate): VectorStat<R> = FilterVectorStat(this, predicate)

package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.math.VectorView

/** Force every update through this stat to use a constant [weight], discarding caller weight. */
fun <R : Result> SeriesStat<R>.withWeight(weight: Double): SeriesStat<R> = WithWeightStat(this, weight)

/** Paired-stat counterpart of [SeriesStat.withWeight]. */
fun <R : Result> PairedStat<R>.withWeight(weight: Double): PairedStat<R> = WithWeightPairedStat(this, weight)

/** Vector-stat counterpart of [SeriesStat.withWeight]. */
fun <R : Result> VectorStat<R>.withWeight(weight: Double): VectorStat<R> = WithWeightVectorStat(this, weight)

/** Discrete-stat counterpart of [SeriesStat.withWeight]. */
fun <R : Result> DiscreteStat<R>.withWeight(weight: Double): DiscreteStat<R> = WithWeightDiscreteStat(this, weight)

/** Adapter implementing the series variant of [withWeight]. */
internal class WithWeightStat<R : Result>(private val delegate: SeriesStat<R>, private val weight: Double) :
    SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, this.weight)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithWeightStat(delegate.create(concurrency), weight)
}

/** Adapter implementing the paired variant of [withWeight]. */
internal class WithWeightPairedStat<R : Result>(private val delegate: PairedStat<R>, private val weight: Double) :
    PairedStat<R>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, y, timestampNanos, this.weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        WithWeightPairedStat(delegate.create(concurrency), weight)
}

/** Adapter implementing the vector variant of [withWeight]. */
internal class WithWeightVectorStat<R : Result>(private val delegate: VectorStat<R>, private val weight: Double) :
    VectorStat<R>,
    Stat<R> by delegate {
    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        delegate.update(vector, timestampNanos, this.weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        WithWeightVectorStat(delegate.create(concurrency), weight)
}

/** Adapter implementing the discrete variant of [withWeight]. */
internal class WithWeightDiscreteStat<R : Result>(private val delegate: DiscreteStat<R>, private val weight: Double) :
    DiscreteStat<R>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, this.weight)
    }

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        WithWeightDiscreteStat(delegate.create(concurrency), weight)
}

/**
 * Per-update weight multiplier driven by the input. `weightBy` multiplies the
 * caller-supplied weight by the value [weighter] returns, so it composes with
 * [withWeight] (which replaces weight outright). The spec-layer counterpart in
 * `Operations.kt` takes a [com.eignex.kumulant.schema.ScalarExpr] and materializes the closure at build
 * time.
 */
fun <R : Result> SeriesStat<R>.weightBy(weighter: (Double) -> Double): SeriesStat<R> =
    WeightBySeriesStat(this, weighter)

/** Paired-stat counterpart of [SeriesStat.weightBy]; [weighter] sees `(x, y)`. */
fun <R : Result> PairedStat<R>.weightBy(weighter: (Double, Double) -> Double): PairedStat<R> =
    WeightByPairedStat(this, weighter)

/** Vector-stat counterpart of [SeriesStat.weightBy]; [weighter] sees the full vector. */
fun <R : Result> VectorStat<R>.weightBy(weighter: (DoubleArray) -> Double): VectorStat<R> =
    WeightByVectorStat(this, weighter)

/** Discrete-stat counterpart of [SeriesStat.weightBy]; [weighter] sees the long value. */
fun <R : Result> DiscreteStat<R>.weightBy(weighter: (Long) -> Double): DiscreteStat<R> =
    WeightByDiscreteStat(this, weighter)

internal class WeightBySeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val weighter: (Double) -> Double,
) : SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, weight * weighter(value))
    }
    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        WeightBySeriesStat(delegate.create(concurrency), weighter)
}

internal class WeightByPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val weighter: (Double, Double) -> Double,
) : PairedStat<R>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, y, timestampNanos, weight * weighter(x, y))
    }
    override fun create(concurrency: Concurrency?): PairedStat<R> =
        WeightByPairedStat(delegate.create(concurrency), weighter)
}

internal class WeightByVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val weighter: (DoubleArray) -> Double,
) : VectorStat<R>,
    Stat<R> by delegate {
    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        delegate.update(vector, timestampNanos, weight * weighter(vector.toDoubleArray()))
    }
    override fun create(concurrency: Concurrency?): VectorStat<R> =
        WeightByVectorStat(delegate.create(concurrency), weighter)
}

internal class WeightByDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val weighter: (Long) -> Double,
) : DiscreteStat<R>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, weight * weighter(value))
    }
    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        WeightByDiscreteStat(delegate.create(concurrency), weighter)
}

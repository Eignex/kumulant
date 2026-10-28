package com.eignex.kumulant.operation

import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.core.isInertWeight
import kotlin.time.Duration

// Inert caller weights pass through unchanged to preserve the no-op contract.
internal fun <R : Result> SeriesStat<R>.withWeight(weight: Double): SeriesStat<R> = WithWeightStat(this, weight)

internal fun <R : Result> PairedStat<R>.withWeight(weight: Double): PairedStat<R> = WithWeightPairedStat(this, weight)

internal fun <R : Result> VectorStat<R>.withWeight(weight: Double): VectorStat<R> = WithWeightVectorStat(this, weight)

internal fun <R : Result> DiscreteStat<R>.withWeight(weight: Double): DiscreteStat<R> = WithWeightDiscreteStat(
    this,
    weight,
)

/**
 * An inert weight stays inert; see [withWeight]. `internal` rather than file-private so every
 * modality adapter shares one definition, including `WithWeightRegressionStat` in `RegressionOps.kt`.
 */
@Suppress("NOTHING_TO_INLINE") // as with isInertWeight; the non-JVM targets pay for the call
internal inline fun Double.orInert(replacement: Double): Double = if (isInertWeight()) this else replacement

internal class WithWeightStat<R : Result>(private val delegate: SeriesStat<R>, private val weight: Double) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, weight.orInert(this.weight))
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        WithWeightStat(delegate.windowed(duration, slices, concurrency), weight)

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithWeightStat(delegate.create(concurrency), weight)
}

internal class WithWeightPairedStat<R : Result>(private val delegate: PairedStat<R>, private val weight: Double) :
    PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, y, timestampNanos, weight.orInert(this.weight))
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        WithWeightPairedStat(delegate.windowed(duration, slices, concurrency), weight)

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        WithWeightPairedStat(delegate.create(concurrency), weight)
}

internal class WithWeightVectorStat<R : Result>(private val delegate: VectorStat<R>, private val weight: Double) :
    VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        delegate.update(vector, timestampNanos, weight.orInert(this.weight))
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        WithWeightVectorStat(delegate.windowed(duration, slices, concurrency), weight)

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        WithWeightVectorStat(delegate.create(concurrency), weight)
}

internal class WithWeightDiscreteStat<R : Result>(private val delegate: DiscreteStat<R>, private val weight: Double) :
    DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, weight.orInert(this.weight))
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        WithWeightDiscreteStat(delegate.windowed(duration, slices, concurrency), weight)

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        WithWeightDiscreteStat(delegate.create(concurrency), weight)
}

internal fun <R : Result> SeriesStat<R>.weightBy(weighter: (Double) -> Double): SeriesStat<R> =
    WeightBySeriesStat(this, weighter)

internal fun <R : Result> PairedStat<R>.weightBy(weighter: (Double, Double) -> Double): PairedStat<R> =
    WeightByPairedStat(this, weighter)

internal fun <R : Result> VectorStat<R>.weightBy(weighter: (DoubleArray) -> Double): VectorStat<R> =
    WeightByVectorStat(this, weighter)

internal fun <R : Result> DiscreteStat<R>.weightBy(weighter: (Long) -> Double): DiscreteStat<R> =
    WeightByDiscreteStat(this, weighter)

internal class WeightBySeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val weighter: (Double) -> Double,
) : SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, weight * weighter(value))
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        WeightBySeriesStat(delegate.windowed(duration, slices, concurrency), weighter)

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        WeightBySeriesStat(delegate.create(concurrency), weighter)
}

internal class WeightByPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val weighter: (Double, Double) -> Double,
) : PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, y, timestampNanos, weight * weighter(x, y))
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        WeightByPairedStat(delegate.windowed(duration, slices, concurrency), weighter)

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        WeightByPairedStat(delegate.create(concurrency), weighter)
}

internal class WeightByVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val weighter: (DoubleArray) -> Double = { 1.0 },
    private val vectorWeighter: ((Vector) -> Double)? = null,
) : VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        delegate.update(
            vector,
            timestampNanos,
            weight * (vectorWeighter?.invoke(vector) ?: weighter(vector.toDoubleArray())),
        )
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        WeightByVectorStat(delegate.windowed(duration, slices, concurrency), weighter, vectorWeighter)

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        WeightByVectorStat(delegate.create(concurrency), weighter, vectorWeighter)

    companion object {
        fun <R : Result> vector(delegate: VectorStat<R>, weighter: (Vector) -> Double): WeightByVectorStat<R> =
            WeightByVectorStat(delegate, vectorWeighter = weighter)
    }
}

internal class WeightByDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val weighter: (Long) -> Double,
) : DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, weight * weighter(value))
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        WeightByDiscreteStat(delegate.windowed(duration, slices, concurrency), weighter)

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        WeightByDiscreteStat(delegate.create(concurrency), weighter)
}

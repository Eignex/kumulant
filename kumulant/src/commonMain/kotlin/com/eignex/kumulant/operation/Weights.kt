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

/**
 * An inert weight stays inert; see [withWeight]. `internal` rather than file-private so every
 * modality adapter shares one definition, including the regression adapter in `RegressionOps.kt`.
 */
@Suppress("NOTHING_TO_INLINE") // as with isInertWeight; the non-JVM targets pay for the call
internal inline fun Double.orInert(replacement: Double): Double = if (isInertWeight()) this else replacement

// withWeight replaces the caller weight, except that an inert caller weight passes through unchanged to
// preserve the no-op contract. weightBy scales the caller weight by a function of the input.

internal fun <R : Result> SeriesStat<R>.withWeight(weight: Double): SeriesStat<R> =
    ReweightSeriesStat(this) { callerWeight, _ -> callerWeight.orInert(weight) }

internal fun <R : Result> PairedStat<R>.withWeight(weight: Double): PairedStat<R> =
    ReweightPairedStat(this) { callerWeight, _, _ -> callerWeight.orInert(weight) }

internal fun <R : Result> VectorStat<R>.withWeight(weight: Double): VectorStat<R> =
    ReweightVectorStat(this) { callerWeight, _ -> callerWeight.orInert(weight) }

internal fun <R : Result> DiscreteStat<R>.withWeight(weight: Double): DiscreteStat<R> =
    ReweightDiscreteStat(this) { callerWeight, _ -> callerWeight.orInert(weight) }

internal fun <R : Result> SeriesStat<R>.weightBy(weighter: (Double) -> Double): SeriesStat<R> =
    ReweightSeriesStat(this) { callerWeight, input -> callerWeight * weighter(input) }

internal fun <R : Result> PairedStat<R>.weightBy(weighter: (Double, Double) -> Double): PairedStat<R> =
    ReweightPairedStat(this) { callerWeight, x, y -> callerWeight * weighter(x, y) }

internal fun <R : Result> VectorStat<R>.weightBy(weighter: (Vector) -> Double): VectorStat<R> =
    ReweightVectorStat(this) { callerWeight, input -> callerWeight * weighter(input) }

internal fun <R : Result> DiscreteStat<R>.weightBy(weighter: (Long) -> Double): DiscreteStat<R> =
    ReweightDiscreteStat(this) { callerWeight, input -> callerWeight * weighter(input) }

// reweight maps (caller weight, input) to the weight the delegate sees.
internal class ReweightSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val reweight: (Double, Double) -> Double,
) : SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, reweight(weight, value))
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        ReweightSeriesStat(delegate.windowed(duration, slices, concurrency), reweight)

    override fun create(concurrency: Concurrency?): SeriesStat<R> = ReweightSeriesStat(
        delegate.create(concurrency),
        reweight,
    )
}

// reweight maps (caller weight, input) to the weight the delegate sees.
internal class ReweightPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val reweight: (Double, Double, Double) -> Double,
) : PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, y, timestampNanos, reweight(weight, x, y))
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        ReweightPairedStat(delegate.windowed(duration, slices, concurrency), reweight)

    override fun create(concurrency: Concurrency?): PairedStat<R> = ReweightPairedStat(
        delegate.create(concurrency),
        reweight,
    )
}

// reweight maps (caller weight, input) to the weight the delegate sees.
internal class ReweightVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val reweight: (Double, Vector) -> Double,
) : VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        delegate.update(vector, timestampNanos, reweight(weight, vector))
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        ReweightVectorStat(delegate.windowed(duration, slices, concurrency), reweight)

    override fun create(concurrency: Concurrency?): VectorStat<R> = ReweightVectorStat(
        delegate.create(concurrency),
        reweight,
    )
}

// reweight maps (caller weight, input) to the weight the delegate sees.
internal class ReweightDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val reweight: (Double, Long) -> Double,
) : DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, reweight(weight, value))
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        ReweightDiscreteStat(delegate.windowed(duration, slices, concurrency), reweight)

    override fun create(concurrency: Concurrency?): DiscreteStat<R> = ReweightDiscreteStat(
        delegate.create(concurrency),
        reweight,
    )
}

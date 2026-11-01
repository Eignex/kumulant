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

// Inert weights are dropped before advancing a gate, preserving the phase of later updates.

internal fun <R : Result> SeriesStat<R>.throttle(every: Int): SeriesStat<R> = GatedSeriesStat(this, ThrottlePolicy(every))

internal fun <R : Result> PairedStat<R>.throttle(every: Int): PairedStat<R> = GatedPairedStat(this, ThrottlePolicy(every))

internal fun <R : Result> VectorStat<R>.throttle(every: Int): VectorStat<R> = GatedVectorStat(this, ThrottlePolicy(every))

internal fun <R : Result> DiscreteStat<R>.throttle(every: Int): DiscreteStat<R> = GatedDiscreteStat(this, ThrottlePolicy(every))

internal fun <R : Result> SeriesStat<R>.sample(rate: Double, seed: Long): SeriesStat<R> =
    GatedSeriesStat(this, SamplePolicy(rate, seed))

internal fun <R : Result> PairedStat<R>.sample(rate: Double, seed: Long): PairedStat<R> =
    GatedPairedStat(this, SamplePolicy(rate, seed))

internal fun <R : Result> VectorStat<R>.sample(rate: Double, seed: Long): VectorStat<R> =
    GatedVectorStat(this, SamplePolicy(rate, seed))

internal fun <R : Result> DiscreteStat<R>.sample(rate: Double, seed: Long): DiscreteStat<R> =
    GatedDiscreteStat(this, SamplePolicy(rate, seed))

internal class GatedSeriesStat<R : Result>(private val delegate: SeriesStat<R>, private val policy: GatePolicy) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    private val gate = policy.open(delegate.concurrency)
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        GatedSeriesStat(delegate.windowed(duration, slices, concurrency), policy)

    override fun create(concurrency: Concurrency?): SeriesStat<R> = GatedSeriesStat(delegate.create(concurrency), gate.childPolicy())
}

internal class GatedPairedStat<R : Result>(private val delegate: PairedStat<R>, private val policy: GatePolicy) :
    PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by delegate {
    private val gate = policy.open(delegate.concurrency)
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(x, y, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        GatedPairedStat(delegate.windowed(duration, slices, concurrency), policy)

    override fun create(concurrency: Concurrency?): PairedStat<R> = GatedPairedStat(delegate.create(concurrency), gate.childPolicy())
}

internal class GatedVectorStat<R : Result>(private val delegate: VectorStat<R>, private val policy: GatePolicy) :
    VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    private val gate = policy.open(delegate.concurrency)
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(vector, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        GatedVectorStat(delegate.windowed(duration, slices, concurrency), policy)

    override fun create(concurrency: Concurrency?): VectorStat<R> = GatedVectorStat(delegate.create(concurrency), gate.childPolicy())
}

internal class GatedDiscreteStat<R : Result>(private val delegate: DiscreteStat<R>, private val policy: GatePolicy) :
    DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    private val gate = policy.open(delegate.concurrency)
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        GatedDiscreteStat(delegate.windowed(duration, slices, concurrency), policy)

    override fun create(concurrency: Concurrency?): DiscreteStat<R> = GatedDiscreteStat(delegate.create(concurrency), gate.childPolicy())
}

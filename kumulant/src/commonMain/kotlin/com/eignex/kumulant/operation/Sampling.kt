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

internal fun <R : Result> SeriesStat<R>.throttle(every: Int): SeriesStat<R> = ThrottleSeriesStat(this, every)

internal fun <R : Result> PairedStat<R>.throttle(every: Int): PairedStat<R> = ThrottlePairedStat(this, every)

internal fun <R : Result> VectorStat<R>.throttle(every: Int): VectorStat<R> = ThrottleVectorStat(this, every)

internal fun <R : Result> DiscreteStat<R>.throttle(every: Int): DiscreteStat<R> = ThrottleDiscreteStat(this, every)

internal fun <R : Result> SeriesStat<R>.sample(rate: Double, seed: Long): SeriesStat<R> =
    SampleSeriesStat(this, rate, seed)

internal fun <R : Result> PairedStat<R>.sample(rate: Double, seed: Long): PairedStat<R> =
    SamplePairedStat(this, rate, seed)

internal fun <R : Result> VectorStat<R>.sample(rate: Double, seed: Long): VectorStat<R> =
    SampleVectorStat(this, rate, seed)

internal fun <R : Result> DiscreteStat<R>.sample(rate: Double, seed: Long): DiscreteStat<R> =
    SampleDiscreteStat(this, rate, seed)

internal fun checkEvery(every: Int) = require(every >= 1) { "throttle every must be >= 1, got $every" }

internal fun checkRate(rate: Double) = require(rate in 0.0..1.0) { "sample rate must be in [0, 1], got $rate" }

internal class ThrottleSeriesStat<R : Result>(private val delegate: SeriesStat<R>, private val every: Int) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        ThrottleSeriesStat(delegate.windowed(duration, slices, concurrency), every)

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        ThrottleSeriesStat(delegate.create(concurrency), every)
}

internal class ThrottlePairedStat<R : Result>(private val delegate: PairedStat<R>, private val every: Int) :
    PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by delegate {
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(x, y, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        ThrottlePairedStat(delegate.windowed(duration, slices, concurrency), every)

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        ThrottlePairedStat(delegate.create(concurrency), every)
}

internal class ThrottleVectorStat<R : Result>(private val delegate: VectorStat<R>, private val every: Int) :
    VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(vector, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        ThrottleVectorStat(delegate.windowed(duration, slices, concurrency), every)

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        ThrottleVectorStat(delegate.create(concurrency), every)
}

internal class ThrottleDiscreteStat<R : Result>(private val delegate: DiscreteStat<R>, private val every: Int) :
    DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        ThrottleDiscreteStat(delegate.windowed(duration, slices, concurrency), every)

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        ThrottleDiscreteStat(delegate.create(concurrency), every)
}

internal class SampleSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val rate: Double,
    private val seed: Long,
) : SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, seed, delegate.concurrency)
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        SampleSeriesStat(delegate.windowed(duration, slices, concurrency), rate, seed)

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        SampleSeriesStat(delegate.create(concurrency), rate, gate.childSeed())
}

internal class SamplePairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val rate: Double,
    private val seed: Long,
) : PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, seed, delegate.concurrency)
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(x, y, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        SamplePairedStat(delegate.windowed(duration, slices, concurrency), rate, seed)

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        SamplePairedStat(delegate.create(concurrency), rate, gate.childSeed())
}

internal class SampleVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val rate: Double,
    private val seed: Long,
) : VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, seed, delegate.concurrency)
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(vector, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        SampleVectorStat(delegate.windowed(duration, slices, concurrency), rate, seed)

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        SampleVectorStat(delegate.create(concurrency), rate, gate.childSeed())
}

internal class SampleDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val rate: Double,
    private val seed: Long,
) : DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, seed, delegate.concurrency)
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        SampleDiscreteStat(delegate.windowed(duration, slices, concurrency), rate, seed)

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        SampleDiscreteStat(delegate.create(concurrency), rate, gate.childSeed())
}

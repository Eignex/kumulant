package com.eignex.kumulant.operation

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.core.isInertWeight
import kotlin.time.Duration

// Throttle / sample adapters. Each wraps an inner stat, intercepts at the update
// boundary, and drops some updates before they reach the delegate. State per
// wrapper is one counter cell, held by the gate.
//
// Both gates are phase-carrying state, so both wrappers drop an inert weight before
// consulting them: an observation that carries no multiplicity must not shift which
// later updates survive. See [Stat] for the guarantee and `Gates.kt` for the gates.
//
// The constructed spec-layer counterparts live in
// `com.eignex.kumulant.schema.Operations.kt`: `throttle(every)` and
// `sample(rate, seed)`.

/** Forward only every [every]th update to the delegate; drop the rest. */
internal fun <R : Result> SeriesStat<R>.throttle(every: Int): SeriesStat<R> = ThrottleSeriesStat(this, every)

/** Paired-stat counterpart of [SeriesStat.throttle]. */
internal fun <R : Result> PairedStat<R>.throttle(every: Int): PairedStat<R> = ThrottlePairedStat(this, every)

/** Vector-stat counterpart of [SeriesStat.throttle]. */
internal fun <R : Result> VectorStat<R>.throttle(every: Int): VectorStat<R> = ThrottleVectorStat(this, every)

/** Discrete-stat counterpart of [SeriesStat.throttle]. */
internal fun <R : Result> DiscreteStat<R>.throttle(every: Int): DiscreteStat<R> = ThrottleDiscreteStat(this, every)

/** Bernoulli-sample each update at probability [rate], drawing from [seed]. */
internal fun <R : Result> SeriesStat<R>.sample(rate: Double, seed: Long): SeriesStat<R> =
    SampleSeriesStat(this, rate, seed)

/** Paired-stat counterpart of [SeriesStat.sample]. */
internal fun <R : Result> PairedStat<R>.sample(rate: Double, seed: Long): PairedStat<R> =
    SamplePairedStat(this, rate, seed)

/** Vector-stat counterpart of [SeriesStat.sample]. */
internal fun <R : Result> VectorStat<R>.sample(rate: Double, seed: Long): VectorStat<R> =
    SampleVectorStat(this, rate, seed)

/** Discrete-stat counterpart of [SeriesStat.sample]. */
internal fun <R : Result> DiscreteStat<R>.sample(rate: Double, seed: Long): DiscreteStat<R> =
    SampleDiscreteStat(this, rate, seed)

/**
 * Reject a throttle period that would forward nothing or everything unpredictably.
 *
 * `internal` rather than file-private so the regression modality's wrappers in `RegressionOps.kt`
 * can share it.
 */
internal fun checkEvery(every: Int) = require(every >= 1) { "throttle every must be >= 1, got $every" }

/** Reject a sampling rate outside the unit interval; see [checkEvery] on why this is `internal`. */
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
    override fun update(vector: F64VectorLike, timestampNanos: Long, weight: Double) {
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

/** Bernoulli sampling helper. [SampleGate] draws from [seed] through a counter cell, so the
 *  gate is safe to share across threads and every copy gets its own derived stream. */
internal class SampleSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val rate: Double,
    private val seed: Long,
) : SeriesStat<R>,
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

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        SampleSeriesStat(delegate.create(concurrency), rate, gate.childSeed())
}

internal class SamplePairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val rate: Double,
    private val seed: Long,
) : PairedStat<R>,
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

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        SamplePairedStat(delegate.create(concurrency), rate, gate.childSeed())
}

internal class SampleVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val rate: Double,
    private val seed: Long,
) : VectorStat<R>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, seed, delegate.concurrency)
    override fun update(vector: F64VectorLike, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(vector, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        SampleVectorStat(delegate.create(concurrency), rate, gate.childSeed())
}

internal class SampleDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val rate: Double,
    private val seed: Long,
) : DiscreteStat<R>,
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

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        SampleDiscreteStat(delegate.create(concurrency), rate, gate.childSeed())
}

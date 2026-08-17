package com.eignex.kumulant.operation

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import kotlin.random.Random

// Throttle / sample adapters. Each wraps an inner stat, intercepts at the update
// boundary, and drops some updates before they reach the delegate. State per
// wrapper is one atomic counter (throttle) or one [Random] reference (sample).
//
// The constructed spec-layer counterparts live in
// `com.eignex.kumulant.schema.Operations.kt`: `throttle(every)` and
// `sample(rate, seed)`. The live forms below take a Kotlin [Random] so the
// caller can plug in whatever PRNG they want.

/** Forward only every [every]th update to the delegate; drop the rest. */
internal fun <R : Result> SeriesStat<R>.throttle(every: Int): SeriesStat<R> = ThrottleSeriesStat(this, every)

/** Paired-stat counterpart of [SeriesStat.throttle]. */
internal fun <R : Result> PairedStat<R>.throttle(every: Int): PairedStat<R> = ThrottlePairedStat(this, every)

/** Vector-stat counterpart of [SeriesStat.throttle]. */
internal fun <R : Result> VectorStat<R>.throttle(every: Int): VectorStat<R> = ThrottleVectorStat(this, every)

/** Discrete-stat counterpart of [SeriesStat.throttle]. */
internal fun <R : Result> DiscreteStat<R>.throttle(every: Int): DiscreteStat<R> = ThrottleDiscreteStat(this, every)

/** Bernoulli-sample each update at probability [rate], using [random] as the PRNG. */
internal fun <R : Result> SeriesStat<R>.sample(rate: Double, random: Random): SeriesStat<R> =
    SampleSeriesStat(this, rate, random)

/** Paired-stat counterpart of [SeriesStat.sample]. */
internal fun <R : Result> PairedStat<R>.sample(rate: Double, random: Random): PairedStat<R> =
    SamplePairedStat(this, rate, random)

/** Vector-stat counterpart of [SeriesStat.sample]. */
internal fun <R : Result> VectorStat<R>.sample(rate: Double, random: Random): VectorStat<R> =
    SampleVectorStat(this, rate, random)

/** Discrete-stat counterpart of [SeriesStat.sample]. */
internal fun <R : Result> DiscreteStat<R>.sample(rate: Double, random: Random): DiscreteStat<R> =
    SampleDiscreteStat(this, rate, random)

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
    Stat<R> by delegate {
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        ThrottleSeriesStat(delegate.create(concurrency), every)
}

internal class ThrottlePairedStat<R : Result>(private val delegate: PairedStat<R>, private val every: Int) :
    PairedStat<R>,
    Stat<R> by delegate {
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(x, y, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        ThrottlePairedStat(delegate.create(concurrency), every)
}

internal class ThrottleVectorStat<R : Result>(private val delegate: VectorStat<R>, private val every: Int) :
    VectorStat<R>,
    Stat<R> by delegate {
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(vector, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        ThrottleVectorStat(delegate.create(concurrency), every)
}

internal class ThrottleDiscreteStat<R : Result>(private val delegate: DiscreteStat<R>, private val every: Int) :
    DiscreteStat<R>,
    Stat<R> by delegate {
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        ThrottleDiscreteStat(delegate.create(concurrency), every)
}

/** Bernoulli sampling helper. The [random] reference is shared across calls; caller
 *  is responsible for thread-safety; the spec materialiser hands each stat its own
 *  fresh `Random(seed)`. */
internal class SampleSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val rate: Double,
    private val random: Random,
) : SeriesStat<R>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, random)
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        SampleSeriesStat(delegate.create(concurrency), rate, random)
}

internal class SamplePairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val rate: Double,
    private val random: Random,
) : PairedStat<R>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, random)
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(x, y, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): PairedStat<R> =
        SamplePairedStat(delegate.create(concurrency), rate, random)
}

internal class SampleVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val rate: Double,
    private val random: Random,
) : VectorStat<R>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, random)
    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(vector, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): VectorStat<R> =
        SampleVectorStat(delegate.create(concurrency), rate, random)
}

internal class SampleDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val rate: Double,
    private val random: Random,
) : DiscreteStat<R>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, random)
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(value, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        SampleDiscreteStat(delegate.create(concurrency), rate, random)
}

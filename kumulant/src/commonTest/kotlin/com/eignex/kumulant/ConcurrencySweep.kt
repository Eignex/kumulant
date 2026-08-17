package com.eignex.kumulant

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.test.assertEquals

/**
 * The cross-mode agreement sweep, which eleven test files were each spelling out for themselves.
 *
 * Every [Concurrency] mode is a different set of state cells behind the same recurrence, so under purely
 * sequential updates all of them must produce bit-identical results. That is the cheap check that catches
 * a mode branch wired to the wrong cell type or missing a lock, before the bench-side tests go looking for
 * real contention.
 *
 * The invariant was stated forty-nine times as an inline `getValue(Concurrency.None)` plus a loop. Written
 * out that often it stops reading as an invariant and starts reading as boilerplate, which is how a file
 * ends up quietly comparing only one field. Naming it makes the sweep something a new stat family opts
 * into rather than re-derives.
 */
internal fun <R> assertModesAgree(label: String, reads: Map<Concurrency, R>) {
    val ref = reads.getValue(Concurrency.None)
    for ((mode, r) in reads) assertEquals(ref, r, "$label mode=$mode")
}

/** Builds one stat per mode with [factory] and drives each through [train], keyed by the mode used. */
internal fun <S, R> readsPerMode(factory: (Concurrency) -> S, train: (S) -> R): Map<Concurrency, R> =
    Concurrency.entries.associateWith { train(factory(it)) }

/**
 * Six values on a one-second grid.
 *
 * The fixture for stats whose recurrence is driven by the clock: the spacing has to be uneven enough that
 * a decay factor computed per-observation differs from one computed per-interval, or a mode that confused
 * the two would still agree.
 */
internal val TIMED_VALUES: DoubleArray = doubleArrayOf(1.0, 2.5, -1.0, 3.0, 0.5, 4.0)

/** The timestamps [TIMED_VALUES] arrive at, in nanoseconds. */
internal val TIMED_STAMPS: LongArray =
    longArrayOf(0L, 1_000_000_000L, 2_000_000_000L, 3_000_000_000L, 4_000_000_000L, 5_000_000_000L)

/**
 * Eight values with eight varied weights.
 *
 * The fixture for stats that ignore the clock. It carries a zero, two negatives and a fractional weight
 * on purpose, so a mode that dropped the weight argument or clamped it would show up rather than pass.
 */
internal val WEIGHTED_VALUES: DoubleArray = doubleArrayOf(1.0, -2.0, 3.5, 0.0, 4.2, -1.1, 7.0, 2.5)

/** The weights [WEIGHTED_VALUES] arrive with. */
internal val WEIGHTED_WEIGHTS: DoubleArray = doubleArrayOf(1.0, 2.0, 1.0, 3.0, 1.0, 1.0, 2.5, 0.5)

/** Feeds [TIMED_VALUES] at [TIMED_STAMPS] with unit weight, reading at the last stamp. */
internal fun <R : Result> timedReads(factory: (Concurrency) -> SeriesStat<R>): Map<Concurrency, R> =
    readsPerMode(factory) { s ->
        for (i in TIMED_VALUES.indices) s.update(TIMED_VALUES[i], TIMED_STAMPS[i], 1.0)
        s.read(TIMED_STAMPS.last())
    }

/** Feeds [WEIGHTED_VALUES] with [WEIGHTED_WEIGHTS], all at timestamp zero. */
internal fun <R : Result> weightedReads(factory: (Concurrency) -> SeriesStat<R>): Map<Concurrency, R> =
    readsPerMode(factory) { s ->
        for (i in WEIGHTED_VALUES.indices) s.update(WEIGHTED_VALUES[i], 0L, WEIGHTED_WEIGHTS[i])
        s.read(0L)
    }

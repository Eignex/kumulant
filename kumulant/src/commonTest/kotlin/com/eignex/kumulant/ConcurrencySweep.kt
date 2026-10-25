package com.eignex.kumulant

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.test.assertEquals

internal fun <R> assertModesAgree(label: String, reads: Map<Concurrency, R>) {
    val ref = reads.getValue(Concurrency.None)
    for ((mode, r) in reads) assertEquals(ref, r, "$label mode=$mode")
}

internal fun <S, R> readsPerMode(factory: (Concurrency) -> S, train: (S) -> R): Map<Concurrency, R> =
    Concurrency.entries.associateWith { train(factory(it)) }

internal val TIMED_VALUES: DoubleArray = doubleArrayOf(1.0, 2.5, -1.0, 3.0, 0.5, 4.0)

internal val TIMED_STAMPS: LongArray =
    longArrayOf(0L, 1_000_000_000L, 2_000_000_000L, 3_000_000_000L, 4_000_000_000L, 5_000_000_000L)

internal val WEIGHTED_VALUES: DoubleArray = doubleArrayOf(1.0, -2.0, 3.5, 0.0, 4.2, -1.1, 7.0, 2.5)

internal val WEIGHTED_WEIGHTS: DoubleArray = doubleArrayOf(1.0, 2.0, 1.0, 3.0, 1.0, 1.0, 2.5, 0.5)

internal fun <R : Result> timedReads(factory: (Concurrency) -> SeriesStat<R>): Map<Concurrency, R> =
    readsPerMode(factory) { s ->
        for (i in TIMED_VALUES.indices) s.update(TIMED_VALUES[i], TIMED_STAMPS[i], 1.0)
        s.read(TIMED_STAMPS.last())
    }

internal fun <R : Result> weightedReads(factory: (Concurrency) -> SeriesStat<R>): Map<Concurrency, R> =
    readsPerMode(factory) { s ->
        for (i in WEIGHTED_VALUES.indices) s.update(WEIGHTED_VALUES[i], 0L, WEIGHTED_WEIGHTS[i])
        s.read(0L)
    }

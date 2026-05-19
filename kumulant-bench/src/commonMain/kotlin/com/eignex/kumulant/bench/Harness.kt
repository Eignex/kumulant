package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.random.Random

/** Single observation passed to a stat under test. */
class Update(val value: Double, val weight: Double, val timestampNanos: Long)

/**
 * Generic spec describing how to drive a univariate [SeriesStat] under the bench
 * module's three test categories (correctness, concurrency, perf). One [StatSpec]
 * instance is reusable across all three drivers.
 *
 * - [factory] constructs a fresh stat at a given [Concurrency] level.
 * - [updates] generates a sequence of [Update]s for a given seed and workload size —
 *   kept deterministic so correctness and concurrency tests share reference values.
 * - [readAt] returns the timestamp at which the snapshot is taken (rate stats need
 *   this to be past the final update so elapsed time is positive).
 * - [scalar] reduces a snapshot to a single Double that correctness tests assert
 *   against a reference computed by [reference].
 * - [tolerance] is the absolute slack allowed when comparing snapshot scalars to
 *   the reference (some stats — sketches in particular — are inherently approximate).
 * - [orderIndependent] = false marks stats whose recurrence folds updates in arrival
 *   order (EWMA family); the concurrency test then only checks finiteness rather
 *   than exact reference match for non-None levels.
 */
class StatSpec<R : Result>(
    val name: String,
    val factory: (Concurrency) -> SeriesStat<R>,
    val updates: (seed: Int, n: Int) -> Sequence<Update>,
    val scalar: (R) -> Double,
    val reference: (Sequence<Update>) -> Double,
    val tolerance: Double = 0.0,
    val readAt: (n: Int) -> Long = { 0L },
    val orderIndependent: Boolean = true,
) {
    /** Run a single-threaded workload and return the snapshot scalar. */
    fun runSerial(seed: Int, n: Int, concurrency: Concurrency = Concurrency.None): Double {
        val stat = factory(concurrency)
        for (u in updates(seed, n)) {
            stat.update(u.value, u.timestampNanos, u.weight)
        }
        return scalar(stat.read(readAt(n)))
    }

    /** Compute the reference for the same workload — exact-math expected value. */
    fun expected(seed: Int, n: Int): Double = reference(updates(seed, n))
}

/** Standard workload: uniform [0, 1) values, unit weights, all at t=0. */
fun uniformUnitWeights(seed: Int, n: Int): Sequence<Update> = sequence {
    val rng = Random(seed)
    repeat(n) { yield(Update(rng.nextDouble(), 1.0, 0L)) }
}

/** Standard workload: uniform [0, 1) values with random weights in [0.5, 1.5), at t=0. */
fun uniformVariableWeights(seed: Int, n: Int): Sequence<Update> = sequence {
    val rng = Random(seed)
    repeat(n) { yield(Update(rng.nextDouble(), 0.5 + rng.nextDouble(), 0L)) }
}

/**
 * Time-progressing workload: monotonically increasing timestamps in 1 ms steps,
 * value in [0, 1), unit weight. Suitable for rate-style stats.
 */
fun timeProgressingUnitWeights(seed: Int, n: Int): Sequence<Update> = sequence {
    val rng = Random(seed)
    val stride = 1_000_000L // 1 ms per update
    repeat(n) { i -> yield(Update(rng.nextDouble(), 1.0, (i + 1).toLong() * stride)) }
}

/** Total elapsed nanoseconds for a [timeProgressingUnitWeights] stream of length [n]. */
fun timeProgressingElapsedNanos(n: Int): Long = (n + 1).toLong() * 1_000_000L

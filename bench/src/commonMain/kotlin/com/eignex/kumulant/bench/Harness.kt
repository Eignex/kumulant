package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.random.Random

/**
 * Generic spec describing how to drive a univariate [SeriesStat] under the bench
 * module's three test categories (correctness, concurrency, perf). One [StatSpec]
 * instance is reusable across all three drivers.
 *
 * - [factory] constructs a fresh stat at a given [Concurrency] level.
 * - [updates] generates a sequence of (value, weight) pairs for a given seed and
 *   workload size — kept deterministic so correctness and concurrency tests share
 *   reference values.
 * - [scalar] reduces a snapshot to a single Double that correctness tests assert
 *   against a reference computed by [reference].
 * - [tolerance] is the absolute slack allowed when comparing snapshot scalars to
 *   the reference (some stats — sketches in particular — are inherently approximate).
 */
class StatSpec<R : Result>(
    val name: String,
    val factory: (Concurrency) -> SeriesStat<R>,
    val updates: (seed: Int, n: Int) -> Sequence<DoubleArray>,
    val scalar: (R) -> Double,
    val reference: (Sequence<DoubleArray>) -> Double,
    val tolerance: Double = 0.0,
) {
    /** Run a single-threaded workload and return the snapshot scalar. */
    fun runSerial(seed: Int, n: Int, concurrency: Concurrency = Concurrency.None): Double {
        val stat = factory(concurrency)
        for (pair in updates(seed, n)) {
            stat.update(pair[0], 0L, pair[1])
        }
        return scalar(stat.read(0L))
    }

    /** Compute the reference for the same workload — exact-math expected value. */
    fun expected(seed: Int, n: Int): Double = reference(updates(seed, n))
}

/** Standard workload: uniform [0, 1) values, unit weights. */
fun uniformUnitWeights(seed: Int, n: Int): Sequence<DoubleArray> = sequence {
    val rng = Random(seed)
    repeat(n) { yield(doubleArrayOf(rng.nextDouble(), 1.0)) }
}

/** Standard workload: uniform [0, 1) values with random weights in [0.5, 1.5). */
fun uniformVariableWeights(seed: Int, n: Int): Sequence<DoubleArray> = sequence {
    val rng = Random(seed)
    repeat(n) { yield(doubleArrayOf(rng.nextDouble(), 0.5 + rng.nextDouble())) }
}

package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.DenseVector
import kotlin.random.Random

/** Single observation passed to a stat under test. */
class Update(val value: Double, val weight: Double, val timestampNanos: Long)

/**
 * Generic spec describing how to drive a stat under the bench module's three test
 * categories (correctness, concurrency, perf). One [StatSpec] is reusable across all
 * three drivers. Parameterised by the live stat type [S] so [SeriesStat]-based
 * summary stats and [DiscreteStat]-based cardinality stats fit the same harness.
 *
 * Build a SeriesStat-backed spec via [seriesStatSpec]; a DiscreteStat-backed one via
 * [discreteStatSpec]. Construct [StatSpec] directly only when wiring a stat type
 * that doesn't have a helper yet.
 */
class StatSpec<S, R : Result>(
    val name: String,
    val factory: (Concurrency) -> S,
    val applyUpdate: (S, Update) -> Unit,
    val readSnapshot: (S, Long) -> R,
    val updates: (seed: Int, n: Int) -> Sequence<Update>,
    val scalar: (R) -> Double,
    val reference: (Sequence<Update>) -> Double,
    val tolerance: Double = 0.0,
    val readAt: (n: Int) -> Long = { 0L },
    /** When false, concurrent execution may produce a different result than the
     *  analytical reference because the stat's recurrence folds updates in arrival
     *  order (EWMA family). The concurrency test then skips the exact-match check
     *  for this spec and only verifies finiteness. */
    val orderIndependent: Boolean = true,
) {
    /** Run a single-threaded workload and return the snapshot scalar. */
    fun runSerial(seed: Int, n: Int, concurrency: Concurrency = Concurrency.None): Double {
        val stat = factory(concurrency)
        for (u in updates(seed, n)) applyUpdate(stat, u)
        return scalar(readSnapshot(stat, readAt(n)))
    }

    /** Compute the reference for the same workload — exact-math expected value. */
    fun expected(seed: Int, n: Int): Double = reference(updates(seed, n))
}

// === Helpers ================================================================

/** Build a spec for a [SeriesStat]-shaped stat (the common case). */
fun <R : Result> seriesStatSpec(
    name: String,
    factory: (Concurrency) -> SeriesStat<R>,
    updates: (seed: Int, n: Int) -> Sequence<Update>,
    scalar: (R) -> Double,
    reference: (Sequence<Update>) -> Double,
    tolerance: Double = 0.0,
    readAt: (n: Int) -> Long = { 0L },
    orderIndependent: Boolean = true,
): StatSpec<SeriesStat<R>, R> = StatSpec(
    name = name,
    factory = factory,
    applyUpdate = { s, u -> s.update(u.value, u.timestampNanos, u.weight) },
    readSnapshot = { s, ts -> s.read(ts) },
    updates = updates,
    scalar = scalar,
    reference = reference,
    tolerance = tolerance,
    readAt = readAt,
    orderIndependent = orderIndependent,
)

/**
 * Build a spec for a [PairedStat]-shaped stat (covariance, AUC, score losses).
 * The y coordinate is derived from [Update.value] by [deriveY], which defaults
 * to `2 * x + 0.1` — a known-slope linear relation suitable for regression and
 * covariance, and a valid logit-style input for the loss family.
 */
fun <R : Result> pairedStatSpec(
    name: String,
    factory: (Concurrency) -> PairedStat<R>,
    updates: (seed: Int, n: Int) -> Sequence<Update>,
    scalar: (R) -> Double,
    reference: (Sequence<Update>) -> Double,
    tolerance: Double,
    deriveY: (Double) -> Double = { 2.0 * it + 0.1 },
    orderIndependent: Boolean = true,
): StatSpec<PairedStat<R>, R> = StatSpec(
    name = name,
    factory = factory,
    applyUpdate = { s, u -> s.update(u.value, deriveY(u.value), u.timestampNanos, u.weight) },
    readSnapshot = { s, ts -> s.read(ts) },
    updates = updates,
    scalar = scalar,
    reference = reference,
    tolerance = tolerance,
    orderIndependent = orderIndependent,
)

/**
 * Build a spec for a [RegressionStat]-shaped stat. The feature vector is the
 * single-element `[Update.value]`, and the target is derived by [deriveY]
 * (defaulting to `2 * x + 0.1` for a known true slope).
 */
fun <R : Result> regressionStatSpec(
    name: String,
    factory: (Concurrency) -> RegressionStat<R>,
    updates: (seed: Int, n: Int) -> Sequence<Update>,
    scalar: (R) -> Double,
    reference: (Sequence<Update>) -> Double,
    tolerance: Double,
    deriveY: (Double) -> Double = { 2.0 * it + 0.1 },
    orderIndependent: Boolean = true,
): StatSpec<RegressionStat<R>, R> = StatSpec(
    name = name,
    factory = factory,
    applyUpdate = { s, u ->
        s.update(DenseVector.of(doubleArrayOf(u.value)), deriveY(u.value), u.timestampNanos, u.weight)
    },
    readSnapshot = { s, ts -> s.read(ts) },
    updates = updates,
    scalar = scalar,
    reference = reference,
    tolerance = tolerance,
    orderIndependent = orderIndependent,
)

/**
 * Build a spec for a [DiscreteStat]-shaped stat (cardinality estimators). The
 * harness [Update.value] is fed to the stat as `value.toRawBits().toLong()` so
 * the bench preserves bit-identical input distributions across stats.
 */
fun <R : Result> discreteStatSpec(
    name: String,
    factory: (Concurrency) -> DiscreteStat<R>,
    updates: (seed: Int, n: Int) -> Sequence<Update>,
    scalar: (R) -> Double,
    reference: (Sequence<Update>) -> Double,
    tolerance: Double,
    orderIndependent: Boolean = true,
): StatSpec<DiscreteStat<R>, R> = StatSpec(
    name = name,
    factory = factory,
    applyUpdate = { s, u -> s.update(u.value.toRawBits(), u.timestampNanos, u.weight) },
    readSnapshot = { s, ts -> s.read(ts) },
    updates = updates,
    scalar = scalar,
    reference = reference,
    tolerance = tolerance,
    orderIndependent = orderIndependent,
)

// === Workloads ==============================================================

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
    val stride = 1_000_000L
    repeat(n) { i -> yield(Update(rng.nextDouble(), 1.0, (i + 1).toLong() * stride)) }
}

/** Total elapsed nanoseconds for a [timeProgressingUnitWeights] stream of length [n]. */
fun timeProgressingElapsedNanos(n: Int): Long = (n + 1).toLong() * 1_000_000L

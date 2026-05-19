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
 * Generic spec describing how to drive a stat under the bench module's analyses.
 * One [StatSpec] feeds the perf benchmark, the accuracy report, and the
 * concurrency-drift report. Parameterised by the live stat type [S] so
 * [SeriesStat]-based summary stats and [DiscreteStat]-based cardinality stats
 * fit the same harness.
 *
 * Build a SeriesStat-backed spec via [seriesStatSpec]; a DiscreteStat-backed one
 * via [discreteStatSpec]. Construct [StatSpec] directly only when wiring a stat
 * type that doesn't have a helper yet.
 */
class StatSpec<S, R : Result>(
    val name: String,
    val factory: (Concurrency) -> S,
    val applyUpdate: (S, Update) -> Unit,
    val readSnapshot: (S, Long) -> R,
    val merge: (S, R) -> Unit,
    val updates: (seed: Int, n: Int) -> Sequence<Update>,
    val scalar: (R) -> Double,
    val reference: (Sequence<Update>) -> Double,
    val readAt: (n: Int) -> Long = { 0L },
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

/** Build a spec for a [SeriesStat]-shaped stat (the common case). */
fun <R : Result> seriesStatSpec(
    name: String,
    factory: (Concurrency) -> SeriesStat<R>,
    updates: (seed: Int, n: Int) -> Sequence<Update>,
    scalar: (R) -> Double,
    reference: (Sequence<Update>) -> Double,
    readAt: (n: Int) -> Long = { 0L },
): StatSpec<SeriesStat<R>, R> = StatSpec(
    name = name,
    factory = factory,
    applyUpdate = { s, u -> s.update(u.value, u.timestampNanos, u.weight) },
    readSnapshot = { s, ts -> s.read(ts) },
    merge = { s, r -> s.merge(r) },
    updates = updates,
    scalar = scalar,
    reference = reference,
    readAt = readAt,
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
    deriveY: (Double) -> Double = { 2.0 * it + 0.1 },
): StatSpec<PairedStat<R>, R> = StatSpec(
    name = name,
    factory = factory,
    applyUpdate = { s, u -> s.update(u.value, deriveY(u.value), u.timestampNanos, u.weight) },
    readSnapshot = { s, ts -> s.read(ts) },
    merge = { s, r -> s.merge(r) },
    updates = updates,
    scalar = scalar,
    reference = reference,
)

/**
 * Build a spec for a [RegressionStat]-shaped stat with [featureSize] features and
 * a known [trueWeights] vector (`length == featureSize`).
 *
 * Each [Update] is expanded into a deterministic random feature vector seeded by
 * `Update.value.toRawBits()`; the target is `trueWeights . x + bias`. This gives
 * every spec a known closed-form regression problem at any featureSize without
 * needing to widen the workload type.
 */
fun <R : Result> regressionStatSpec(
    name: String,
    factory: (Concurrency) -> RegressionStat<R>,
    updates: (seed: Int, n: Int) -> Sequence<Update>,
    scalar: (R) -> Double,
    reference: (Sequence<Update>) -> Double,
    featureSize: Int = 1,
    trueWeights: DoubleArray = doubleArrayOf(2.0),
    bias: Double = 0.1,
): StatSpec<RegressionStat<R>, R> {
    require(trueWeights.size == featureSize) {
        "$name: trueWeights.size=${trueWeights.size} must equal featureSize=$featureSize"
    }
    return StatSpec(
        name = name,
        factory = factory,
        applyUpdate = { s, u ->
            val x = featuresFor(u.value, featureSize)
            var y = bias
            var i = 0
            while (i < featureSize) { y += trueWeights[i] * x[i]; i++ }
            s.update(DenseVector.of(x), y, u.timestampNanos, u.weight)
        },
        readSnapshot = { s, ts -> s.read(ts) },
        merge = { s, r -> s.merge(r) },
        updates = updates,
        scalar = scalar,
        reference = reference,
    )
}

/**
 * Deterministic feature vector seeded by [value]'s bit pattern. The same value
 * always yields the same features so the bench's reference computations are
 * reproducible across runs.
 */
fun featuresFor(value: Double, featureSize: Int): DoubleArray {
    val rng = Random(value.toRawBits())
    return DoubleArray(featureSize) { rng.nextDouble() * 2.0 - 1.0 }
}

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
): StatSpec<DiscreteStat<R>, R> = StatSpec(
    name = name,
    factory = factory,
    applyUpdate = { s, u -> s.update(u.value.toRawBits(), u.timestampNanos, u.weight) },
    readSnapshot = { s, ts -> s.read(ts) },
    merge = { s, r -> s.merge(r) },
    updates = updates,
    scalar = scalar,
    reference = reference,
)

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

/** Inter-update timestamp step used by [timeProgressingUnitWeights]. Exposed so the
 *  concurrency-drift analyzer can shift per-thread workloads onto non-overlapping
 *  time windows for rate stats that silently drop out-of-order timestamps. */
const val TIME_PROGRESSING_STRIDE_NANOS: Long = 1_000_000L

/**
 * Time-progressing workload: monotonically increasing timestamps in 1 ms steps,
 * value in [0, 1), unit weight. Suitable for rate-style stats.
 */
fun timeProgressingUnitWeights(seed: Int, n: Int): Sequence<Update> = sequence {
    val rng = Random(seed)
    repeat(n) { i ->
        yield(Update(rng.nextDouble(), 1.0, (i + 1).toLong() * TIME_PROGRESSING_STRIDE_NANOS))
    }
}

/** Total elapsed nanoseconds for a [timeProgressingUnitWeights] stream of length [n]. */
fun timeProgressingElapsedNanos(n: Int): Long = (n + 1).toLong() * TIME_PROGRESSING_STRIDE_NANOS

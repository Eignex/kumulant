package com.eignex.kumulant.bench

import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stat.decay.DecayingMeanStat
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.DecayingVarianceStat
import com.eignex.kumulant.stat.decay.EwmaMeanStat
import com.eignex.kumulant.stat.decay.EwmaVarianceStat
import com.eignex.kumulant.stat.rate.CounterRateStat
import com.eignex.kumulant.stat.rate.DecayingRateStat
import com.eignex.kumulant.stat.rate.RateStat
import com.eignex.kumulant.stat.summary.BernoulliSumStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.MaxStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MinStat
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.RangeStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.TotalWeightsStat
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.math.exp
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes

/**
 * Registry of [StatSpec]s — one entry per univariate stat. Tests and benchmarks
 * iterate over [allSpecs] (or a category subset) so adding a new stat means adding
 * an entry here and nothing else.
 */

// === Summary ================================================================

private fun bernoulliWorkload(seed: Int, n: Int): Sequence<Update> = sequence {
    val rng = Random(seed)
    repeat(n) {
        yield(Update(if (rng.nextDouble() < 0.3) 1.0 else 0.0, 0.5 + rng.nextDouble(), 0L))
    }
}

private fun twoPassMean(data: List<Update>): Double {
    val totW = data.sumOf { it.weight }
    return if (totW == 0.0) 0.0 else data.sumOf { it.value * it.weight } / totW
}

private fun twoPassVariance(data: List<Update>): Double {
    val totW = data.sumOf { it.weight }
    if (totW == 0.0) return 0.0
    val mean = data.sumOf { it.value * it.weight } / totW
    return data.sumOf { val d = it.value - mean; it.weight * d * d } / totW
}

val sumStatSpec = seriesStatSpec(
    name = "SumStat",
    factory = { c -> SumStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { u -> u.value * u.weight } },
    tolerance = 1e-9,
)

val countStatSpec = seriesStatSpec(
    name = "CountStat",
    factory = { c -> CountStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.count().toDouble() },
    tolerance = 1e-9,
)

val totalWeightsStatSpec = seriesStatSpec(
    name = "TotalWeightsStat",
    factory = { c -> TotalWeightsStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { u -> u.weight } },
    tolerance = 1e-9,
)

val meanStatSpec = seriesStatSpec(
    name = "MeanStat",
    factory = { c -> MeanStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
    tolerance = 1e-9,
)

val varianceStatSpec = seriesStatSpec(
    name = "VarianceStat",
    factory = { c -> VarianceStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { twoPassVariance(it.toList()) },
    tolerance = 1e-9,
)

val momentsStatSpec = seriesStatSpec(
    name = "MomentsStat",
    factory = { c -> MomentsStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
    tolerance = 1e-9,
)

val minStatSpec = seriesStatSpec(
    name = "MinStat",
    factory = { c -> MinStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.min },
    reference = { seq -> seq.fold(Double.POSITIVE_INFINITY) { acc, u -> min(acc, u.value) } },
    tolerance = 0.0,
)

val maxStatSpec = seriesStatSpec(
    name = "MaxStat",
    factory = { c -> MaxStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.max },
    reference = { seq -> seq.fold(Double.NEGATIVE_INFINITY) { acc, u -> max(acc, u.value) } },
    tolerance = 0.0,
)

val rangeStatSpec = seriesStatSpec(
    name = "RangeStat",
    factory = { c -> RangeStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.max - it.min },
    reference = { seq ->
        var lo = Double.POSITIVE_INFINITY
        var hi = Double.NEGATIVE_INFINITY
        for (u in seq) {
            if (u.value < lo) lo = u.value
            if (u.value > hi) hi = u.value
        }
        hi - lo
    },
    tolerance = 0.0,
)

val bernoulliSumStatSpec = seriesStatSpec(
    name = "BernoulliSumStat",
    factory = { c -> BernoulliSumStat(c) },
    updates = ::bernoulliWorkload,
    scalar = { it.successes },
    reference = { it.sumOf { u -> u.value * u.weight } },
    tolerance = 1e-9,
)

// === Decay ==================================================================
//
// Time-driven decay stats are exercised at `timestampNanos = 0` for every update
// and the read — the decay factor `exp(-alpha*(t - t_i))` collapses to 1 so the
// stat behaves like its non-decaying counterpart and admits a closed-form
// reference. EWMA-family stats (decay by accumulated weight) require the
// recursion-based reference and are order-dependent.

private val decayWeighting = DecayWeighting.HalfLife(1.hours)
private val ewmaWeighting = DecayWeighting.Alpha(0.01)

private fun ewmaMean(alpha: Double, data: List<Update>): Double {
    var biased = 0.0
    var cumW = 0.0
    for (u in data) {
        val a = 1.0 - exp(-alpha * u.weight)
        biased += a * (u.value - biased)
        cumW += u.weight
    }
    val bc = 1.0 - exp(-alpha * cumW)
    return if (bc > 0.0) biased / bc else 0.0
}

private fun ewmaVariance(alpha: Double, data: List<Update>): Double {
    var biasedMean = 0.0
    var biasedM2 = 0.0
    var cumW = 0.0
    for (u in data) {
        val a = 1.0 - exp(-alpha * u.weight)
        val delta = u.value - biasedMean
        biasedMean += a * delta
        biasedM2 = (1.0 - a) * (biasedM2 + a * delta * delta)
        cumW += u.weight
    }
    val bc = 1.0 - exp(-alpha * cumW)
    return if (bc > 0.0) biasedM2 / bc else 0.0
}

val decayingSumStatSpec = seriesStatSpec(
    name = "DecayingSumStat",
    factory = { c -> DecayingSumStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { u -> u.value * u.weight } },
    tolerance = 1e-9,
)

val decayingMeanStatSpec = seriesStatSpec(
    name = "DecayingMeanStat",
    factory = { c -> DecayingMeanStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
    tolerance = 1e-9,
)

val decayingVarianceStatSpec = seriesStatSpec(
    name = "DecayingVarianceStat",
    factory = { c -> DecayingVarianceStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { twoPassVariance(it.toList()) },
    tolerance = 1e-9,
)

val ewmaMeanStatSpec = seriesStatSpec(
    name = "EwmaMeanStat",
    factory = { c -> EwmaMeanStat(ewmaWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { ewmaMean(ewmaWeighting.alpha, it.toList()) },
    tolerance = 1e-9,
    orderIndependent = false,
)

val ewmaVarianceStatSpec = seriesStatSpec(
    name = "EwmaVarianceStat",
    factory = { c -> EwmaVarianceStat(ewmaWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { ewmaVariance(ewmaWeighting.alpha, it.toList()) },
    tolerance = 1e-9,
    orderIndependent = false,
)

// === Rate ===================================================================
//
// Rate stats need real elapsed time to produce meaningful values: rate =
// totalValue / elapsed_seconds. The workload progresses timestamps in 1 ms
// strides, and [StatSpec.readAt] takes the snapshot just past the last update.

// RateStat measures elapsed from the *first* observation's timestamp, not from
// zero. Our workload puts the first update at 1 ms, so elapsedSec = readAt - 1ms.
private const val WORKLOAD_STRIDE_NANOS = 1_000_000L

private fun elapsedSeconds(list: List<Update>): Double {
    if (list.isEmpty()) return 0.0
    return (readAtFor(list.size) - list.first().timestampNanos) / 1_000_000_000.0
}

private fun rateReference(seq: Sequence<Update>): Double {
    val list = seq.toList()
    val elapsedSec = elapsedSeconds(list)
    if (elapsedSec <= 0.0) return 0.0
    return list.sumOf { it.value * it.weight } / elapsedSec
}

private fun counterReference(seq: Sequence<Update>): Double {
    val list = seq.toList()
    val elapsedSec = elapsedSeconds(list)
    if (elapsedSec <= 0.0) return 0.0
    return list.last().value / elapsedSec
}

private fun readAtFor(n: Int): Long = timeProgressingElapsedNanos(n)

private val decayingRateHalfLife = 30.minutes

// DecayingRateStat exposes `decayedSum * ln(2)/halfLifeSec`. With our half-life
// (30 minutes) far exceeding the workload's elapsed window (~5–10s), the decay
// factor is ~1 so the snapshot tracks `totalValue * ln(2)/halfLifeSec`.
private fun decayingRateReference(seq: Sequence<Update>): Double {
    val list = seq.toList()
    if (list.isEmpty()) return 0.0
    val total = list.sumOf { it.value * it.weight }
    val scale = kotlin.math.ln(2.0) / (decayingRateHalfLife.inWholeNanoseconds / 1_000_000_000.0)
    return total * scale
}

val rateStatSpec = seriesStatSpec(
    name = "RateStat",
    factory = { c -> RateStat(c) },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.rate },
    reference = ::rateReference,
    readAt = ::readAtFor,
    // Under HighWrite striping, the startTimestamp may be set by a later
    // sample than the actual first, slightly shrinking the elapsed denominator.
    tolerance = 1.0,
)

val decayingRateStatSpec = seriesStatSpec(
    name = "DecayingRateStat",
    factory = { c -> DecayingRateStat(decayingRateHalfLife, c) },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.rate },
    reference = ::decayingRateReference,
    readAt = ::readAtFor,
    // Small decay over the workload window — within 1% of the un-decayed scaled sum.
    tolerance = 1e-2,
)

val counterRateStatSpec = seriesStatSpec(
    name = "CounterRateStat",
    factory = { c -> CounterRateStat(c) },
    updates = ::counterWorkload,
    scalar = { it.rate },
    reference = ::counterReference,
    readAt = ::readAtFor,
    tolerance = 1.0,
    // CounterRate assumes a single monotonic counter source. The concurrency test
    // concatenates per-thread counters which the stat reads as resets — the
    // result depends on the interleaving order, so skip the exact comparison
    // for non-None levels. The serial correctness test still pins the math.
    orderIndependent = false,
)

private fun counterWorkload(seed: Int, n: Int): Sequence<Update> = sequence {
    val rng = Random(seed)
    val stride = 1_000_000L
    var v = 0.0
    repeat(n) { i ->
        v += rng.nextDouble()
        yield(Update(v, 1.0, (i + 1).toLong() * stride))
    }
}

// === Cardinality ============================================================
//
// Cardinality stats consume Long identifiers. The harness converts each Update
// value to its IEEE-754 raw bits — that yields well-spread integer IDs from the
// uniform [0, 1) double workload. Reference cardinality is the count of distinct
// raw-bit IDs in the stream; sketches sit within their stated standard error.

val hyperLogLogStatSpec = discreteStatSpec(
    name = "HyperLogLogStat",
    factory = { c -> com.eignex.kumulant.stat.cardinality.HyperLogLogStat(precision = 14, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.estimate },
    reference = { seq -> seq.map { it.value.toRawBits() }.toSet().size.toDouble() },
    // Standard error ~ 1.04/sqrt(2^14) = 0.81%. With 5000 distinct IDs this is
    // about 40 — allow 100 for safety across seeds and concurrency-induced drift.
    tolerance = 100.0,
)

// === Sketches ===============================================================
//
// Sketch stats check the universal "no update was lost" invariant via totalSeen.
// The dedicated unit tests in :kumulant cover accuracy; the bench test the
// concurrency-safety of the update path.

val bloomFilterStatSpec = discreteStatSpec(
    name = "BloomFilterStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.BloomFilterStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
    tolerance = 0.0,
)

val countMinSketchStatSpec = discreteStatSpec(
    name = "CountMinSketchStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.CountMinSketchStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
    tolerance = 0.0,
)

val minHashStatSpec = discreteStatSpec(
    name = "MinHashStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.MinHashStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
    tolerance = 0.0,
)

val spaceSavingStatSpec = discreteStatSpec(
    name = "SpaceSavingStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.SpaceSavingStat(capacity = 128, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
    tolerance = 0.0,
)

val linearCountingStatSpec = discreteStatSpec(
    name = "LinearCountingStat",
    factory = { c ->
        com.eignex.kumulant.stat.cardinality.LinearCountingStat(bits = 1 shl 16, concurrency = c)
    },
    updates = ::uniformUnitWeights,
    scalar = { it.estimate },
    reference = { seq -> seq.map { it.value.toRawBits() }.toSet().size.toDouble() },
    // 64K-bit bitset over 5000 distinct IDs: load ~7.6%, bias is small and the
    // estimator converges quickly. Allow 50 to be safe.
    tolerance = 50.0,
)

// === Quantile ===============================================================
//
// Quantile stats check "no update was lost" via totalWeights / totalSeen. Frugal
// is order-dependent (random walk) and uses a wide tolerance to allow drift
// around the true median of uniform [0, 1).

val ddSketchStatSpec = seriesStatSpec(
    name = "DDSketchStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.DDSketchStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
    tolerance = 1e-6,
)

val hdrHistogramStatSpec = seriesStatSpec(
    name = "HdrHistogramStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.HdrHistogramStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { snap -> snap.weights.sum() },
    reference = { it.count().toDouble() },
    tolerance = 1e-6,
)

val linearHistogramStatSpec = seriesStatSpec(
    name = "LinearHistogramStat",
    factory = { c ->
        com.eignex.kumulant.stat.quantile.LinearHistogramStat(
            lowerBound = 0.0,
            upperBound = 1.0,
            binCount = 64,
            concurrency = c,
        )
    },
    updates = ::uniformUnitWeights,
    scalar = { snap -> snap.weights.sum() },
    reference = { it.count().toDouble() },
    tolerance = 1e-6,
)

val reservoirHistogramStatSpec = seriesStatSpec(
    name = "ReservoirHistogramStat",
    factory = { c ->
        com.eignex.kumulant.stat.quantile.ReservoirHistogramStat(capacity = 256, concurrency = c)
    },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
    tolerance = 0.0,
)

val tDigestStatSpec = seriesStatSpec(
    name = "TDigestStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.TDigestStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { snap -> snap.weights.sum() },
    reference = { it.count().toDouble() },
    tolerance = 1e-6,
)

val frugalQuantileStatSpec = seriesStatSpec(
    name = "FrugalQuantileStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.FrugalQuantileStat(q = 0.5, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.quantile },
    // Frugal is a random walk targeting q=0.5 over uniform [0,1); the median is
    // 0.5 but the estimate wanders within a few stepSizes of it.
    reference = { _ -> 0.5 },
    tolerance = 0.2,
    orderIndependent = false,
)

/** Every spec exposed by the bench module. */
val allSpecs: List<StatSpec<*, *>> = listOf(
    sumStatSpec,
    countStatSpec,
    totalWeightsStatSpec,
    meanStatSpec,
    varianceStatSpec,
    momentsStatSpec,
    minStatSpec,
    maxStatSpec,
    rangeStatSpec,
    bernoulliSumStatSpec,
    decayingSumStatSpec,
    decayingMeanStatSpec,
    decayingVarianceStatSpec,
    ewmaMeanStatSpec,
    ewmaVarianceStatSpec,
    rateStatSpec,
    decayingRateStatSpec,
    counterRateStatSpec,
    hyperLogLogStatSpec,
    linearCountingStatSpec,
    bloomFilterStatSpec,
    countMinSketchStatSpec,
    minHashStatSpec,
    spaceSavingStatSpec,
    ddSketchStatSpec,
    hdrHistogramStatSpec,
    linearHistogramStatSpec,
    reservoirHistogramStatSpec,
    tDigestStatSpec,
    frugalQuantileStatSpec,
)

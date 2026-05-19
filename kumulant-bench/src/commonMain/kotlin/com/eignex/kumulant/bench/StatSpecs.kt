@file:OptIn(ExperimentalAtomicApi::class)

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
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
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
)

val countStatSpec = seriesStatSpec(
    name = "CountStat",
    factory = { c -> CountStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.count().toDouble() },
)

val totalWeightsStatSpec = seriesStatSpec(
    name = "TotalWeightsStat",
    factory = { c -> TotalWeightsStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { u -> u.weight } },
)

val meanStatSpec = seriesStatSpec(
    name = "MeanStat",
    factory = { c -> MeanStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
)

val varianceStatSpec = seriesStatSpec(
    name = "VarianceStat",
    factory = { c -> VarianceStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { twoPassVariance(it.toList()) },
)

val momentsStatSpec = seriesStatSpec(
    name = "MomentsStat",
    factory = { c -> MomentsStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
)

val minStatSpec = seriesStatSpec(
    name = "MinStat",
    factory = { c -> MinStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.min },
    reference = { seq -> seq.fold(Double.POSITIVE_INFINITY) { acc, u -> min(acc, u.value) } },
)

val maxStatSpec = seriesStatSpec(
    name = "MaxStat",
    factory = { c -> MaxStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.max },
    reference = { seq -> seq.fold(Double.NEGATIVE_INFINITY) { acc, u -> max(acc, u.value) } },
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
)

val pairedSumStatSpec = pairedStatSpec(
    name = "PairedSumStat",
    factory = { c -> com.eignex.kumulant.stat.summary.PairedSumStat(concurrency = c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sumX },
    reference = { it.sumOf { u -> u.value * u.weight } },
)

val bernoulliSumStatSpec = seriesStatSpec(
    name = "BernoulliSumStat",
    factory = { c -> BernoulliSumStat(c) },
    updates = ::bernoulliWorkload,
    scalar = { it.successes },
    reference = { it.sumOf { u -> u.value * u.weight } },
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
)

val decayingMeanStatSpec = seriesStatSpec(
    name = "DecayingMeanStat",
    factory = { c -> DecayingMeanStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
)

val decayingVarianceStatSpec = seriesStatSpec(
    name = "DecayingVarianceStat",
    factory = { c -> DecayingVarianceStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { twoPassVariance(it.toList()) },
)

val ewmaMeanStatSpec = seriesStatSpec(
    name = "EwmaMeanStat",
    factory = { c -> EwmaMeanStat(ewmaWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { ewmaMean(ewmaWeighting.alpha, it.toList()) },
)

val ewmaVarianceStatSpec = seriesStatSpec(
    name = "EwmaVarianceStat",
    factory = { c -> EwmaVarianceStat(ewmaWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { ewmaVariance(ewmaWeighting.alpha, it.toList()) },
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
)

val decayingRateStatSpec = seriesStatSpec(
    name = "DecayingRateStat",
    factory = { c -> DecayingRateStat(decayingRateHalfLife, c) },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.rate },
    reference = ::decayingRateReference,
    readAt = ::readAtFor,
    // Small decay over the workload window — within 1% of the un-decayed scaled sum.
)

// CounterRateStat is semantically one monotonic counter. The bench mirrors that
// by binding the factory's stat to an [AtomicLong] that serializes all writers
// behind a globally-monotonic sequence: each call to [applyUpdate] increments
// the shared counter and feeds its new value to the stat. Under concurrent
// writers the stat now sees true monotonic input regardless of thread
// interleaving, isolating the stat's own concurrency primitives from the
// "two independent counters" misuse pattern.
class CounterRateBag internal constructor(val stat: CounterRateStat, val counter: AtomicLong)

val counterRateStatSpec: StatSpec<CounterRateBag, com.eignex.kumulant.stat.rate.RateResult> = StatSpec(
    name = "CounterRateStat",
    factory = { c ->
        // Multi-writer bench: opt out of decrease-as-reset so out-of-order
        // arrivals from racing threads don't inflate the running delta. See the
        // CounterRateStat class docstring.
        CounterRateBag(CounterRateStat(c, treatDecreaseAsReset = false), AtomicLong(0L))
    },
    applyUpdate = { bag, u ->
        val i = bag.counter.addAndFetch(1L)
        bag.stat.update(i.toDouble(), u.timestampNanos, u.weight)
    },
    readSnapshot = { bag, ts -> bag.stat.read(ts) },
    merge = { bag, r -> bag.stat.merge(r) },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.rate },
    reference = ::counterRateReference,
    readAt = ::readAtFor,
)

private fun counterRateReference(seq: Sequence<Update>): Double {
    val list = seq.toList()
    val elapsedSec = elapsedSeconds(list)
    if (elapsedSec <= 0.0) return 0.0
    // Each update increments the shared counter by 1, so the final absolute
    // counter value equals the stream length regardless of thread interleaving.
    return list.size.toDouble() / elapsedSec
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
)

val countMinSketchStatSpec = discreteStatSpec(
    name = "CountMinSketchStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.CountMinSketchStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
)

val minHashStatSpec = discreteStatSpec(
    name = "MinHashStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.MinHashStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
)

val spaceSavingStatSpec = discreteStatSpec(
    name = "SpaceSavingStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.SpaceSavingStat(capacity = 128, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
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
)

val hdrHistogramStatSpec = seriesStatSpec(
    name = "HdrHistogramStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.HdrHistogramStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { snap -> snap.weights.sum() },
    reference = { it.count().toDouble() },
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
)

val reservoirHistogramStatSpec = seriesStatSpec(
    name = "ReservoirHistogramStat",
    factory = { c ->
        com.eignex.kumulant.stat.quantile.ReservoirHistogramStat(capacity = 256, concurrency = c)
    },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
)

val tDigestStatSpec = seriesStatSpec(
    name = "TDigestStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.TDigestStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { snap -> snap.weights.sum() },
    reference = { it.count().toDouble() },
)

val frugalQuantileStatSpec = seriesStatSpec(
    name = "FrugalQuantileStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.FrugalQuantileStat(q = 0.5, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.quantile },
    // Frugal is a random walk targeting q=0.5 over uniform [0,1); the median is
    // 0.5 but the estimate wanders within a few stepSizes of it.
    reference = { _ -> 0.5 },
)

// === Regression =============================================================
//
// Multi-feature regression specs feed an 8-dim deterministic random feature
// vector (seeded by the update value's bits) and a known target
// y = trueWeights · x + bias. The scalar pulls weights[0] from the snapshot;
// the reference is trueWeights[0] = 2.0. This exercises the full coupled
// (Sxx, Sxy) recurrence rather than the trivial featureSize=1 path.

private const val REG_FEATURE_SIZE = 8
private val regTrueWeights = doubleArrayOf(2.0, -1.0, 0.5, -0.5, 1.0, 0.1, -0.2, 0.3)

val univariateRegressionStatSpec = pairedStatSpec(
    name = "UnivariateRegressionStat",
    factory = { c -> com.eignex.kumulant.stat.regression.UnivariateRegressionStat(concurrency = c) },
    updates = ::uniformVariableWeights,
    scalar = { it.slope },
    reference = { _ -> 2.0 },
)

val covarianceStatSpec = pairedStatSpec(
    name = "CovarianceStat",
    factory = { c -> com.eignex.kumulant.stat.regression.CovarianceStat(concurrency = c) },
    updates = ::uniformVariableWeights,
    scalar = { it.covariance },
    // cov(X, 2X + 0.1) = 2 * var(X). For X ~ U[0,1), var(X) = 1/12 → cov ≈ 0.1667.
    // With weighted samples the empirical variance drifts; allow 5% slack.
    reference = { seq ->
        val data = seq.toList()
        val totW = data.sumOf { it.weight }
        val meanX = data.sumOf { it.value * it.weight } / totW
        val varX = data.sumOf { val d = it.value - meanX; it.weight * d * d } / totW
        2.0 * varX
    },
)

val bayesianRegressionStatSpec = regressionStatSpec(
    name = "BayesianRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.regression.BayesianRegressionStat(
            featureSize = REG_FEATURE_SIZE, concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.weights[0] },
    reference = { _ -> regTrueWeights[0] },
    featureSize = REG_FEATURE_SIZE,
    trueWeights = regTrueWeights,
)

val diagonalRegressionStatSpec = regressionStatSpec(
    name = "DiagonalRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.regression.DiagonalRegressionStat(
            featureSize = REG_FEATURE_SIZE, concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.weights[0] },
    reference = { _ -> regTrueWeights[0] },
    featureSize = REG_FEATURE_SIZE,
    trueWeights = regTrueWeights,
)

val stochasticRegressionStatSpec = regressionStatSpec(
    name = "StochasticRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.regression.StochasticRegressionStat(
            featureSize = REG_FEATURE_SIZE, concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.weights[0] },
    reference = { _ -> regTrueWeights[0] },
    featureSize = REG_FEATURE_SIZE,
    trueWeights = regTrueWeights,
)

// === Score ==================================================================
//
// Score stats consume (prediction, label) pairs. For the bench, prediction =
// Update.value (uniform [0, 1)) and label = `deriveTargetY(x)` clamped where
// the stat needs [0, 1] inputs. We check totalWeights or a coarse score value.

private fun clamped01(x: Double): Double = x.coerceIn(0.0, 1.0)

val aucStatSpec = pairedStatSpec(
    name = "AucStat",
    factory = { c -> com.eignex.kumulant.stat.score.AucStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalPositives + it.totalNegatives },
    reference = { it.count().toDouble() },
    // AucStat takes (score, label) with label in {0, 1}. Map our deriveY to {0, 1}
    // by thresholding so the stat doesn't reject the input.
    deriveY = { if (it > 0.5) 1.0 else 0.0 },
)

val brierScoreStatSpec = pairedStatSpec(
    name = "BrierScoreStat",
    factory = { c -> com.eignex.kumulant.stat.score.BrierScoreStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
    deriveY = { if (it > 0.5) 1.0 else 0.0 },
)

val pinballLossStatSpec = pairedStatSpec(
    name = "PinballLossStat",
    factory = { c -> com.eignex.kumulant.stat.score.PinballLossStat(tau = 0.5, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
    deriveY = ::clamped01,
)

val logLossStatSpec = pairedStatSpec(
    name = "LogLossStat",
    factory = { c -> com.eignex.kumulant.stat.score.LogLossStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
    // LogLoss needs prediction in (0, 1); shift away from the endpoints.
    deriveY = { (it * 0.98 + 0.01).coerceIn(0.001, 0.999) },
)

val maeLossStatSpec = pairedStatSpec(
    name = "MaeLossStat",
    factory = { c -> com.eignex.kumulant.stat.score.MaeLossStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
)

val mseLossStatSpec = pairedStatSpec(
    name = "MseLossStat",
    factory = { c -> com.eignex.kumulant.stat.score.MseLossStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
)

val reliabilityStatSpec = pairedStatSpec(
    name = "ReliabilityStat",
    factory = { c -> com.eignex.kumulant.stat.score.ReliabilityStat(numBins = 16, concurrency = c) },
    updates = ::uniformUnitWeights,
    // Reliability tracks bin-wise counts; total weight in the snapshot's bin
    // histogram should equal the stream size.
    scalar = { snap -> snap.totalWeights.sum() },
    reference = { it.count().toDouble() },
    deriveY = { if (it > 0.5) 1.0 else 0.0 },
)

// === Tree ===================================================================
//
// Tree regressors fold (vector, y, weight) tuples into a piecewise-constant model.
// We don't try to match slope or split structure here; the invariant is
// "every update's weight reached the snapshot", which makes
// [TreeRegressionResult.totalWeights] / [ForestRegressionResult.totalWeights]
// the natural scalar.

private val treeSplitCandidates = listOf(
    com.eignex.kumulant.stat.tree.ThresholdSplit(0, 0.5),
)

val decisionTreeRegressionStatSpec = regressionStatSpec(
    name = "DecisionTreeRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.tree.DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = treeSplitCandidates,
            concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.totalWeights },
    reference = { seq -> seq.sumOf { it.weight } },
)

val randomForestRegressionStatSpec = regressionStatSpec(
    name = "RandomForestRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.tree.RandomForestRegressionStat(
            featureSize = 1,
            splitCandidates = treeSplitCandidates,
            nbrTrees = 4,
            bagging = false,
            concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    // ForestRegressionResult.totalWeights = sum over trees; without bagging each
    // tree absorbs the full stream, so the total is `nbrTrees * sum(weights)`.
    scalar = { it.totalWeights },
    reference = { seq -> 4.0 * seq.sumOf { it.weight } },
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
    pairedSumStatSpec,
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
    univariateRegressionStatSpec,
    covarianceStatSpec,
    bayesianRegressionStatSpec,
    diagonalRegressionStatSpec,
    stochasticRegressionStatSpec,
    aucStatSpec,
    brierScoreStatSpec,
    logLossStatSpec,
    maeLossStatSpec,
    mseLossStatSpec,
    pinballLossStatSpec,
    reliabilityStatSpec,
    decisionTreeRegressionStatSpec,
    randomForestRegressionStatSpec,
)

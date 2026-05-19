package com.eignex.kumulant.bench

import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stat.decay.DecayingMeanStat
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.DecayingVarianceStat
import com.eignex.kumulant.stat.decay.EwmaMeanStat
import com.eignex.kumulant.stat.decay.EwmaVarianceStat
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

/**
 * Registry of [StatSpec]s — one entry per univariate stat. Tests and benchmarks
 * iterate over [allSpecs] (or a category subset) so adding a new stat means adding
 * an entry here and nothing else.
 */

// === Summary ================================================================

private fun bernoulliWorkload(seed: Int, n: Int): Sequence<DoubleArray> = sequence {
    val rng = Random(seed)
    repeat(n) {
        yield(doubleArrayOf(if (rng.nextDouble() < 0.3) 1.0 else 0.0, 0.5 + rng.nextDouble()))
    }
}

private fun twoPassMean(data: List<DoubleArray>): Double {
    val totW = data.sumOf { it[1] }
    return if (totW == 0.0) 0.0 else data.sumOf { it[0] * it[1] } / totW
}

private fun twoPassVariance(data: List<DoubleArray>): Double {
    val totW = data.sumOf { it[1] }
    if (totW == 0.0) return 0.0
    val mean = data.sumOf { it[0] * it[1] } / totW
    return data.sumOf { val d = it[0] - mean; it[1] * d * d } / totW
}

val sumStatSpec = StatSpec(
    name = "SumStat",
    factory = { c -> SumStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { p -> p[0] * p[1] } },
    tolerance = 1e-9,
)

val countStatSpec = StatSpec(
    name = "CountStat",
    factory = { c -> CountStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.count().toDouble() },
    tolerance = 1e-9,
)

val totalWeightsStatSpec = StatSpec(
    name = "TotalWeightsStat",
    factory = { c -> TotalWeightsStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { p -> p[1] } },
    tolerance = 1e-9,
)

val meanStatSpec = StatSpec(
    name = "MeanStat",
    factory = { c -> MeanStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
    tolerance = 1e-9,
)

val varianceStatSpec = StatSpec(
    name = "VarianceStat",
    factory = { c -> VarianceStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { twoPassVariance(it.toList()) },
    tolerance = 1e-9,
)

val momentsStatSpec = StatSpec(
    name = "MomentsStat",
    factory = { c -> MomentsStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
    tolerance = 1e-9,
)

val minStatSpec = StatSpec(
    name = "MinStat",
    factory = { c -> MinStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.min },
    reference = { seq -> seq.fold(Double.POSITIVE_INFINITY) { acc, p -> min(acc, p[0]) } },
    tolerance = 0.0,
)

val maxStatSpec = StatSpec(
    name = "MaxStat",
    factory = { c -> MaxStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.max },
    reference = { seq -> seq.fold(Double.NEGATIVE_INFINITY) { acc, p -> max(acc, p[0]) } },
    tolerance = 0.0,
)

val rangeStatSpec = StatSpec(
    name = "RangeStat",
    factory = { c -> RangeStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.max - it.min },
    reference = { seq ->
        var lo = Double.POSITIVE_INFINITY
        var hi = Double.NEGATIVE_INFINITY
        for (p in seq) {
            if (p[0] < lo) lo = p[0]
            if (p[0] > hi) hi = p[0]
        }
        hi - lo
    },
    tolerance = 0.0,
)

// === Decay ==================================================================
//
// All time-driven decay stats are exercised at `timestampNanos = 0` for every
// update and the read — the decay factor `exp(-alpha*(t - t_i))` collapses to 1,
// so the stat behaves like its non-decaying counterpart and admits a closed-form
// reference. EWMA-family stats (decay by accumulated weight) require the
// recursion-based reference and are order-dependent.

// A 1-hour half-life is irrelevant when every update lands at t=0, but the stat
// requires *some* schedule at construction.
private val decayWeighting = DecayWeighting.HalfLife(1.hours)
private val ewmaWeighting = DecayWeighting.Alpha(0.01)

private fun ewmaMean(alpha: Double, data: List<DoubleArray>): Double {
    var biased = 0.0
    var cumW = 0.0
    for (p in data) {
        val a = 1.0 - exp(-alpha * p[1])
        biased += a * (p[0] - biased)
        cumW += p[1]
    }
    val bc = 1.0 - exp(-alpha * cumW)
    return if (bc > 0.0) biased / bc else 0.0
}

private fun ewmaVariance(alpha: Double, data: List<DoubleArray>): Double {
    var biasedMean = 0.0
    var biasedM2 = 0.0
    var cumW = 0.0
    for (p in data) {
        val a = 1.0 - exp(-alpha * p[1])
        val delta = p[0] - biasedMean
        biasedMean += a * delta
        biasedM2 = (1.0 - a) * (biasedM2 + a * delta * delta)
        cumW += p[1]
    }
    val bc = 1.0 - exp(-alpha * cumW)
    return if (bc > 0.0) biasedM2 / bc else 0.0
}

val decayingSumStatSpec = StatSpec(
    name = "DecayingSumStat",
    factory = { c -> DecayingSumStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { p -> p[0] * p[1] } },
    tolerance = 1e-9,
)

val decayingMeanStatSpec = StatSpec(
    name = "DecayingMeanStat",
    factory = { c -> DecayingMeanStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
    tolerance = 1e-9,
)

val decayingVarianceStatSpec = StatSpec(
    name = "DecayingVarianceStat",
    factory = { c -> DecayingVarianceStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { twoPassVariance(it.toList()) },
    tolerance = 1e-9,
)

val ewmaMeanStatSpec = StatSpec(
    name = "EwmaMeanStat",
    factory = { c -> EwmaMeanStat(ewmaWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { ewmaMean(ewmaWeighting.alpha, it.toList()) },
    tolerance = 1e-9,
    orderIndependent = false,
)

val ewmaVarianceStatSpec = StatSpec(
    name = "EwmaVarianceStat",
    factory = { c -> EwmaVarianceStat(ewmaWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { ewmaVariance(ewmaWeighting.alpha, it.toList()) },
    tolerance = 1e-9,
    orderIndependent = false,
)

val bernoulliSumStatSpec = StatSpec(
    name = "BernoulliSumStat",
    factory = { c -> BernoulliSumStat(c) },
    updates = ::bernoulliWorkload,
    scalar = { it.successes },
    reference = { it.sumOf { p -> p[0] * p[1] } },
    tolerance = 1e-9,
)

/** Every spec exposed by the bench module. */
val allSpecs: List<StatSpec<*>> = listOf(
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
)

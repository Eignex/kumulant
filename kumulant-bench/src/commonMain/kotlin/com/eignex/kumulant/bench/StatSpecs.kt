package com.eignex.kumulant.bench

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
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

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
)

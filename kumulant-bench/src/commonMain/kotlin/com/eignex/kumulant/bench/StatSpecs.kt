package com.eignex.kumulant.bench

import com.eignex.kumulant.stat.summary.SumStat
import kotlin.math.max
import kotlin.math.min

/**
 * Registry of [StatSpec]s — one entry per univariate stat. Tests and benchmarks
 * iterate over [allSpecs] (or a category subset) so adding a new stat means adding
 * an entry here and nothing else.
 */

val sumStatSpec = StatSpec(
    name = "SumStat",
    factory = { c -> SumStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.sum },
    reference = { updates -> updates.sumOf { it[0] * it[1] } },
    tolerance = 1e-9,
)

/** Every spec exposed by the bench module. */
val allSpecs: List<StatSpec<*>> = listOf(
    sumStatSpec,
)

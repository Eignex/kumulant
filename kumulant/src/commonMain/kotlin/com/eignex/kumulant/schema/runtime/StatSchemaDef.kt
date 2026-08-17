package com.eignex.kumulant.schema.runtime

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.*
import com.eignex.kumulant.schema.spec.*
import kotlinx.serialization.Serializable

/**
 * Pure-data, serializable form of a [StatSchema]. The wire field is `stats`
 * (kumulant convention, customised over skema's default `entries`). Decode an
 * incoming wire payload into this type and rehydrate a live group via
 * [materializeSeries] (or one of the modality-specific variants).
 */
@Serializable
data class StatSchemaDef(
    /** Per-entry specs keyed by [StatKey.name]. */
    val stats: Map<String, StatSpec>,
)

/** Materialize every entry, regardless of modality. Caller filters by stat type. */
fun StatSchemaDef.materialize(concurrency: Concurrency = Concurrency.None): List<BoundStat<*, *, *>> =
    stats.map { (name, config) -> bind(name, config.materialize(concurrency)) }

/** Materialize series-modality entries only; throws if any entry isn't series. */
fun StatSchemaDef.materializeSeries(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out SeriesStat<*>, *>> = stats.map { (name, config) ->
    require(config is SeriesStatSpec<*>) {
        "Entry '$name' has config ${config::class.simpleName}, expected a SeriesStatSpec"
    }
    bindSeries(name, config.materialize(concurrency))
}

/** Materialize paired-modality entries only; throws if any entry isn't paired. */
fun StatSchemaDef.materializePaired(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out PairedStat<*>, *>> = stats.map { (name, config) ->
    require(config is PairedStatSpec<*>) {
        "Entry '$name' has config ${config::class.simpleName}, expected a PairedStatSpec"
    }
    bindPaired(name, config.materialize(concurrency))
}

/** Materialize vector-modality entries only; throws if any entry isn't vector. */
fun StatSchemaDef.materializeVector(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out VectorStat<*>, *>> = stats.map { (name, config) ->
    require(config is VectorStatSpec<*>) {
        "Entry '$name' has config ${config::class.simpleName}, expected a VectorStatSpec"
    }
    bindVector(name, config.materialize(concurrency))
}

/** Materialize discrete-modality entries only; throws if any entry isn't discrete. */
fun StatSchemaDef.materializeDiscrete(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out DiscreteStat<*>, *>> = stats.map { (name, config) ->
    require(config is DiscreteStatSpec<*>) {
        "Entry '$name' has config ${config::class.simpleName}, expected a DiscreteStatSpec"
    }
    bindDiscrete(name, config.materialize(concurrency))
}

// All five of these are `toSpec(StatKey(name), stat)` spelled out longhand - same package, same cast,
// same suppression. Delegating drops five copies of the unchecked cast along with the five annotations
// justifying it, and leaves one place where the erasure argument has to hold.
private fun bind(name: String, stat: Stat<*>): BoundStat<*, *, *> = toSpec(StatKey<Result>(name), stat)

private fun bindSeries(name: String, stat: SeriesStat<*>): BoundStat<*, out SeriesStat<*>, *> =
    toSpec(StatKey<Result>(name), stat)

private fun bindPaired(name: String, stat: PairedStat<*>): BoundStat<*, out PairedStat<*>, *> =
    toSpec(StatKey<Result>(name), stat)

private fun bindVector(name: String, stat: VectorStat<*>): BoundStat<*, out VectorStat<*>, *> =
    toSpec(StatKey<Result>(name), stat)

private fun bindDiscrete(name: String, stat: DiscreteStat<*>): BoundStat<*, out DiscreteStat<*>, *> =
    toSpec(StatKey<Result>(name), stat)

package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
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

@Suppress("UNCHECKED_CAST")
private fun bind(name: String, stat: Stat<*>): BoundStat<*, *, *> =
    BoundStat(StatKey<Result>(name), stat as Stat<Result>) as BoundStat<*, *, *>

@Suppress("UNCHECKED_CAST")
private fun bindSeries(name: String, stat: SeriesStat<*>): BoundStat<*, out SeriesStat<*>, *> =
    BoundStat(StatKey<Result>(name), stat as SeriesStat<Result>) as BoundStat<*, out SeriesStat<*>, *>

@Suppress("UNCHECKED_CAST")
private fun bindPaired(name: String, stat: PairedStat<*>): BoundStat<*, out PairedStat<*>, *> =
    BoundStat(StatKey<Result>(name), stat as PairedStat<Result>) as BoundStat<*, out PairedStat<*>, *>

@Suppress("UNCHECKED_CAST")
private fun bindVector(name: String, stat: VectorStat<*>): BoundStat<*, out VectorStat<*>, *> =
    BoundStat(StatKey<Result>(name), stat as VectorStat<Result>) as BoundStat<*, out VectorStat<*>, *>

@Suppress("UNCHECKED_CAST")
private fun bindDiscrete(name: String, stat: DiscreteStat<*>): BoundStat<*, out DiscreteStat<*>, *> =
    BoundStat(StatKey<Result>(name), stat as DiscreteStat<Result>) as BoundStat<*, out DiscreteStat<*>, *>

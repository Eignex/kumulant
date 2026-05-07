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
data class StatSchemaDef(val stats: Map<String, StatConfig>)

/** Materialize every entry, regardless of modality. Caller filters by stat type. */
fun StatSchemaDef.materialize(concurrency: Concurrency = Concurrency.None): List<StatSpec<*, *, *>> =
    stats.map { (name, config) -> bind(name, config.materialize(concurrency)) }

/** Materialize series-modality entries only; throws if any entry isn't series. */
fun StatSchemaDef.materializeSeries(
    concurrency: Concurrency = Concurrency.None,
): List<StatSpec<*, out SeriesStat<*>, *>> =
    stats.map { (name, config) ->
        require(config is SeriesStatConfig<*>) {
            "Entry '$name' has config ${config::class.simpleName}, expected a SeriesStatConfig"
        }
        bindSeries(name, config.materialize(concurrency))
    }

/** Materialize paired-modality entries only; throws if any entry isn't paired. */
fun StatSchemaDef.materializePaired(
    concurrency: Concurrency = Concurrency.None,
): List<StatSpec<*, out PairedStat<*>, *>> =
    stats.map { (name, config) ->
        require(config is PairedStatConfig<*>) {
            "Entry '$name' has config ${config::class.simpleName}, expected a PairedStatConfig"
        }
        bindPaired(name, config.materialize(concurrency))
    }

/** Materialize vector-modality entries only; throws if any entry isn't vector. */
fun StatSchemaDef.materializeVector(
    concurrency: Concurrency = Concurrency.None,
): List<StatSpec<*, out VectorStat<*>, *>> =
    stats.map { (name, config) ->
        require(config is VectorStatConfig<*>) {
            "Entry '$name' has config ${config::class.simpleName}, expected a VectorStatConfig"
        }
        bindVector(name, config.materialize(concurrency))
    }

/** Materialize discrete-modality entries only; throws if any entry isn't discrete. */
fun StatSchemaDef.materializeDiscrete(
    concurrency: Concurrency = Concurrency.None,
): List<StatSpec<*, out DiscreteStat<*>, *>> =
    stats.map { (name, config) ->
        require(config is DiscreteStatConfig<*>) {
            "Entry '$name' has config ${config::class.simpleName}, expected a DiscreteStatConfig"
        }
        bindDiscrete(name, config.materialize(concurrency))
    }

@Suppress("UNCHECKED_CAST")
private fun bind(name: String, stat: Stat<*>): StatSpec<*, *, *> =
    StatSpec(StatKey<Result>(name), stat as Stat<Result>) as StatSpec<*, *, *>

@Suppress("UNCHECKED_CAST")
private fun bindSeries(name: String, stat: SeriesStat<*>): StatSpec<*, out SeriesStat<*>, *> =
    StatSpec(StatKey<Result>(name), stat as SeriesStat<Result>) as StatSpec<*, out SeriesStat<*>, *>

@Suppress("UNCHECKED_CAST")
private fun bindPaired(name: String, stat: PairedStat<*>): StatSpec<*, out PairedStat<*>, *> =
    StatSpec(StatKey<Result>(name), stat as PairedStat<Result>) as StatSpec<*, out PairedStat<*>, *>

@Suppress("UNCHECKED_CAST")
private fun bindVector(name: String, stat: VectorStat<*>): StatSpec<*, out VectorStat<*>, *> =
    StatSpec(StatKey<Result>(name), stat as VectorStat<Result>) as StatSpec<*, out VectorStat<*>, *>

@Suppress("UNCHECKED_CAST")
private fun bindDiscrete(name: String, stat: DiscreteStat<*>): StatSpec<*, out DiscreteStat<*>, *> =
    StatSpec(StatKey<Result>(name), stat as DiscreteStat<Result>) as StatSpec<*, out DiscreteStat<*>, *>

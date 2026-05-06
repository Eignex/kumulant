package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import kotlinx.serialization.Serializable

/** A named entry in a [StatSchemaDef]: pairs a property-style name with its [StatConfig]. */
@Serializable
data class NamedStatConfig(val name: String, val config: StatConfig)

/**
 * Pure-data, serializable form of a [StatSchema]. Produced by
 * [StatSchema.definition] and the wire value sent over HTTP / written to YAML
 * cloud configs.
 *
 * Use [materialize] (or one of its modality-specific variants) with a
 * [Concurrency] choice to rebuild a list of [StatSpec]s for a [StatGroup].
 */
@Serializable
data class StatSchemaDef(val stats: List<NamedStatConfig>) {

    /** Materialize every entry, regardless of modality. Caller filters by stat type. */
    fun materialize(concurrency: Concurrency = Concurrency.None): List<StatSpec<*, *, *>> =
        stats.map { (name, config) -> bind(name, config.materialize(concurrency)) }

    /** Materialize series-modality entries only; throws if any entry isn't series. */
    fun materializeSeries(concurrency: Concurrency = Concurrency.None): List<StatSpec<*, out SeriesStat<*>, *>> =
        stats.map { (name, config) ->
            require(config is SeriesStatConfig<*>) {
                "Entry '$name' has config ${config::class.simpleName}, expected a SeriesStatConfig"
            }
            bindSeries(name, config.materialize(concurrency))
        }

    /** Materialize paired-modality entries only; throws if any entry isn't paired. */
    fun materializePaired(concurrency: Concurrency = Concurrency.None): List<StatSpec<*, out PairedStat<*>, *>> =
        stats.map { (name, config) ->
            require(config is PairedStatConfig<*>) {
                "Entry '$name' has config ${config::class.simpleName}, expected a PairedStatConfig"
            }
            bindPaired(name, config.materialize(concurrency))
        }

    /** Materialize vector-modality entries only; throws if any entry isn't vector. */
    fun materializeVector(concurrency: Concurrency = Concurrency.None): List<StatSpec<*, out VectorStat<*>, *>> =
        stats.map { (name, config) ->
            require(config is VectorStatConfig<*>) {
                "Entry '$name' has config ${config::class.simpleName}, expected a VectorStatConfig"
            }
            bindVector(name, config.materialize(concurrency))
        }

    /** Materialize discrete-modality entries only; throws if any entry isn't discrete. */
    fun materializeDiscrete(concurrency: Concurrency = Concurrency.None): List<StatSpec<*, out DiscreteStat<*>, *>> =
        stats.map { (name, config) ->
            require(config is DiscreteStatConfig<*>) {
                "Entry '$name' has config ${config::class.simpleName}, expected a DiscreteStatConfig"
            }
            bindDiscrete(name, config.materialize(concurrency))
        }

    /**
     * Bind this wire definition back to a typed [StatSchema] subclass [T] that
     * both producer and consumer share as code. Returns the typed [schema]
     * (for `snap[schema.someProperty]`-style typed lookups) paired with a live
     * [StatGroup] materialized from the wire data.
     *
     * Verifies that the wire definition matches what [factory] produces — drift
     * (renamed/missing/added entries, changed config defaults) fails here at
     * hydration time, not later at use site.
     *
     * The [factory] runs in [StatSchema.skeleton] mode so its delegates
     * collect typed keys + the schema definition without allocating a
     * parallel set of unused live stats — only the wire-materialized
     * [StatGroup] in the returned [TypedSchema] carries live state.
     */
    fun <T : StatSchema> bindTo(
        factory: () -> T,
        concurrency: Concurrency = Concurrency.None,
    ): TypedSchema<T> {
        val schema = StatSchema.skeleton(factory)
        val expected = schema.definition()
        require(this == expected) {
            "Wire schema differs from ${schema::class.simpleName}: expected $expected, got $this"
        }
        val group = StatGroup(stats = materializeSeries(concurrency), concurrency = concurrency)
        return TypedSchema(schema, group)
    }
}

/** Result of [StatSchemaDef.bindTo]: a typed [StatSchema] paired with its live [StatGroup]. */
data class TypedSchema<T : StatSchema>(val schema: T, val group: StatGroup)

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

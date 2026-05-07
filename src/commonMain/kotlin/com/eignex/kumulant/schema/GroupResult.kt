package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.skema.Schema
import kotlinx.serialization.Serializable

/** Aggregated snapshot keyed by [StatKey.name]; use `get` operators for typed lookup. */
@Serializable
data class GroupResult(
    val results: Map<String, Result>,
) : Result {
    @Suppress("UNCHECKED_CAST")
    operator fun <R : Result> get(key: StatKey<R>): R {
        val value = requireNotNull(results[key.name]) {
            "Result key '${key.name}' not found. Available: ${results.keys}"
        }
        return value as R
    }

    operator fun <R : Result> get(group: StatKey<GroupResult>, key: StatKey<R>): R {
        return this[group][key]
    }

    operator fun <K> get(group: GroupStatKey<K>): GroupResult {
        return this[group as StatKey<GroupResult>]
    }

    operator fun <K, R : Result> get(group: GroupStatKey<K>, key: StatKey<R>): R {
        return this[group][key]
    }

    operator fun <K, R : Result> get(group: GroupStatKey<K>, select: K.() -> StatKey<R>): R {
        return this[group][group.keys.select()]
    }
}

/**
 * Declarative, typed schema for a group of stats, layered on top of
 * `com.eignex.skema.Schema<StatConfig>` so the entries map is wire-serializable.
 *
 * Subclass and declare stats via the [series], [paired], [vector], [discrete], [raw],
 * and [group] delegates; each property exposes a [StatKey] for typed retrieval from a
 * [GroupResult]. Every entry is a [StatConfig], which means the schema always
 * round-trips through the wire — no live-stat back-door.
 *
 * If you need an aggregation that isn't wire-expressible (e.g. a `filter`-wrapped
 * stat), build a [StatGroup] / `*ListStats` directly with the vararg `Pair`
 * constructor — bypass the schema layer entirely.
 *
 * The schema-level [concurrency] is the deployment knob: every config materializes
 * via `config.materialize(concurrency)` inside the [StatGroup] / `*ListStats`
 * constructor.
 */
abstract class StatSchema(val concurrency: Concurrency = Concurrency.None) : Schema<StatConfig>() {

    /** Pure-data, serializable view of this schema using kumulant's wire field `stats`. */
    fun statSchemaDef(): StatSchemaDef = StatSchemaDef(definition().entries)

    protected fun <R : Result> series(config: SeriesStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    protected fun <R : Result> paired(config: PairedStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    protected fun <R : Result> vector(config: VectorStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    protected fun <R : Result> discrete(config: DiscreteStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    protected fun <R : Result> raw(config: RawStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    /** Nest a sub-schema as a [GroupStatConfig]; materialization recurses at the parent's group construction. */
    protected fun <T : StatSchema> group(nestedSchema: T) =
        register(GroupStatConfig(nestedSchema.statSchemaDef().stats)) { GroupStatKey(it, nestedSchema) }
}

/** Series-modality specs from a schema, materialized at the schema's [concurrency][StatSchema.concurrency]. */
internal fun seriesSpecs(schema: StatSchema): List<StatSpec<*, out SeriesStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is SeriesStatConfig<*>) return@mapNotNull null
        toSpec<SeriesStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

/** Paired-modality specs from a schema. */
internal fun pairedSpecs(schema: StatSchema): List<StatSpec<*, out PairedStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is PairedStatConfig<*>) return@mapNotNull null
        toSpec<PairedStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

/** Vector-modality specs from a schema. */
internal fun vectorSpecs(schema: StatSchema): List<StatSpec<*, out VectorStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is VectorStatConfig<*>) return@mapNotNull null
        toSpec<VectorStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

/** Discrete-modality specs from a schema. */
internal fun discreteSpecs(schema: StatSchema): List<StatSpec<*, out DiscreteStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is DiscreteStatConfig<*>) return@mapNotNull null
        toSpec<DiscreteStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

/** Raw-modality specs from a schema (tree histograms, CrpsEnsemble — no fan-out group). */
internal fun rawSpecs(schema: StatSchema): List<StatSpec<*, *, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is RawStatConfig<*>) return@mapNotNull null
        toSpec<Stat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

@Suppress("UNCHECKED_CAST")
internal fun <S : Stat<*>> toSpec(key: StatKey<*>, stat: S): StatSpec<*, out S, *> =
    StatSpec(key as StatKey<Result>, stat as Stat<Result>) as StatSpec<*, out S, *>

internal inline fun <reified S : Stat<*>> filterSpecs(
    specs: List<StatSpec<*, *, *>>
): List<StatSpec<*, out S, *>> =
    specs.mapNotNull { (key, stat) -> if (stat is S) toSpec(key, stat) else null }

@Suppress("UNCHECKED_CAST")
internal fun mergeEntry(
    values: GroupResult,
    key: StatKey<*>,
    stat: Stat<*>
) {
    val result = values.results[key.name] ?: return
    if (result is GroupResult && stat is GroupedStat) {
        stat.merge(result)
        return
    }
    (stat as Stat<Result>).merge(result)
}

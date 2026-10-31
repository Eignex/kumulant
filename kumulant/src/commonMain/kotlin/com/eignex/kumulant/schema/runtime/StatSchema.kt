package com.eignex.kumulant.schema.runtime

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.GroupResult
import com.eignex.kumulant.schema.GroupStatKey
import com.eignex.kumulant.schema.StatKey
import com.eignex.kumulant.schema.spec.DiscreteGroupStatSpec
import com.eignex.kumulant.schema.spec.DiscreteStatSpec
import com.eignex.kumulant.schema.spec.GroupStatSpec
import com.eignex.kumulant.schema.spec.PairedGroupStatSpec
import com.eignex.kumulant.schema.spec.PairedStatSpec
import com.eignex.kumulant.schema.spec.RegressionGroupStatSpec
import com.eignex.kumulant.schema.spec.RegressionStatSpec
import com.eignex.kumulant.schema.spec.SeriesStatSpec
import com.eignex.kumulant.schema.spec.StatSpec
import com.eignex.kumulant.schema.spec.VectorGroupStatSpec
import com.eignex.kumulant.schema.spec.VectorStatSpec
import com.eignex.skema.Schema

/**
 * Declarative, typed schema for a group of stats, layered on top of
 * `com.eignex.skema.Schema<StatSpec>` so the entries map is wire-serializable.
 *
 * Subclass and declare stats via the [series], [paired], [vector], [discrete], and
 * [group] delegates; each property exposes a [StatKey] for typed retrieval from a
 * [GroupResult]. Every entry is a [StatSpec], which means the schema always
 * round-trips through the wire - no live-stat back-door.
 *
 * A schema describes stats and nothing else. [com.eignex.kumulant.core.Concurrency] is a deployment
 * choice, so it is supplied where the stats are built - the [StatGroup] / `*ListStats` constructor -
 * and applies to the whole tree, nested groups included. One schema can therefore back a sharded
 * worker and its coordinator at different levels.
 *
 * @sample com.eignex.kumulant.samples.schemaDeclarationAndRead
 */
@Suppress("AbstractClassCanBeConcreteClass") // concrete class would expose `register` and the modality
// helpers to direct callers; abstract enforces the "extend and declare stats" usage pattern.
abstract class StatSchema : Schema<StatSpec>() {

    /** Pure-data, serializable view of this schema using kumulant's wire field `stats`. */
    fun statSchemaDef(): StatSchemaDef = StatSchemaDef(definition().entries)

    protected fun <R : Result> series(config: SeriesStatSpec<R>) = register(config) { StatKey<R>(it) }

    protected fun <R : Result> paired(config: PairedStatSpec<R>) = register(config) { StatKey<R>(it) }

    protected fun <R : Result> vector(config: VectorStatSpec<R>) = register(config) { StatKey<R>(it) }

    protected fun <R : Result> discrete(config: DiscreteStatSpec<R>) = register(config) { StatKey<R>(it) }

    protected fun <R : Result> regression(config: RegressionStatSpec<R>) = register(config) { StatKey<R>(it) }

    /** Nest a sub-schema whose entries share one input modality, preserving its typed keys. */
    protected fun <T : StatSchema> group(nestedSchema: T) =
        register(groupSpec(nestedSchema)) { GroupStatKey(it, nestedSchema) }
}

private fun groupSpec(nestedSchema: StatSchema): StatSpec {
    val nested = nestedSchema.statSchemaDef().stats
    return when {
        nested.values.all { it is SeriesStatSpec<*> } -> GroupStatSpec(nested)
        nested.values.all { it is PairedStatSpec<*> } -> PairedGroupStatSpec(nested)
        nested.values.all { it is VectorStatSpec<*> } -> VectorGroupStatSpec(nested)
        nested.values.all { it is DiscreteStatSpec<*> } -> DiscreteGroupStatSpec(nested)
        nested.values.all { it is RegressionStatSpec<*> } -> RegressionGroupStatSpec(nested)
        else -> throw IllegalArgumentException("a nested schema must use one input modality")
    }
}

internal fun seriesSpecs(schema: StatSchema, concurrency: Concurrency): List<BoundStat<*, out SeriesStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is SeriesStatSpec<*>) return@mapNotNull null
        name to config.compileSeries(name)
    }.map { (name, plan) -> toSpec<SeriesStat<*>>(StatKey<Result>(name), plan.build(concurrency)) }

internal fun pairedSpecs(schema: StatSchema, concurrency: Concurrency): List<BoundStat<*, out PairedStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is PairedStatSpec<*>) return@mapNotNull null
        name to config.compilePaired(name)
    }.map { (name, plan) -> toSpec<PairedStat<*>>(StatKey<Result>(name), plan.build(concurrency)) }

internal fun vectorSpecs(schema: StatSchema, concurrency: Concurrency): List<BoundStat<*, out VectorStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is VectorStatSpec<*>) return@mapNotNull null
        name to config.compileVector(name)
    }.map { (name, plan) -> toSpec<VectorStat<*>>(StatKey<Result>(name), plan.build(concurrency)) }

internal fun discreteSpecs(schema: StatSchema, concurrency: Concurrency): List<BoundStat<*, out DiscreteStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is DiscreteStatSpec<*>) return@mapNotNull null
        name to config.compileDiscrete(name)
    }.map { (name, plan) -> toSpec<DiscreteStat<*>>(StatKey<Result>(name), plan.build(concurrency)) }

internal fun regressionSpecs(
    schema: StatSchema,
    concurrency: Concurrency,
): List<BoundStat<*, out RegressionStat<*>, *>> = schema.entries.mapNotNull { (name, config) ->
    if (config !is RegressionStatSpec<*>) return@mapNotNull null
    name to config.compileRegression(name)
}.map { (name, plan) -> toSpec<RegressionStat<*>>(StatKey<Result>(name), plan.build(concurrency)) }

@Suppress("UNCHECKED_CAST")
internal fun <S : Stat<*>> toSpec(key: StatKey<*>, stat: S): BoundStat<*, out S, *> =
    BoundStat(key as StatKey<Result>, stat as Stat<Result>) as BoundStat<*, out S, *>

@Suppress("UNCHECKED_CAST")
internal fun mergeEntry(values: GroupResult, key: StatKey<*>, stat: Stat<*>, timestampNanos: Long) {
    val result = values.results[key.name] ?: return
    if (result is GroupResult && stat is GroupedStat) {
        stat.merge(result, timestampNanos)
        return
    }
    (stat as Stat<Result>).merge(result, timestampNanos)
}

@Suppress("UNCHECKED_CAST")
internal fun validateMergeEntry(stat: Stat<*>, result: Result, name: String) {
    try {
        (stat as Stat<Result>).validateMerge(result)
    } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("cannot merge entry '$name': ${e.message}", e)
    }
}

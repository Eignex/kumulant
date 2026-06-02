package com.eignex.kumulant.schema.runtime

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.DiscreteStatSpec
import com.eignex.kumulant.schema.GroupResult
import com.eignex.kumulant.schema.GroupStatKey
import com.eignex.kumulant.schema.GroupStatSpec
import com.eignex.kumulant.schema.PairedStatSpec
import com.eignex.kumulant.schema.RegressionStatSpec
import com.eignex.kumulant.schema.SeriesStatSpec
import com.eignex.kumulant.schema.StatKey
import com.eignex.kumulant.schema.StatSpec
import com.eignex.kumulant.schema.VectorStatSpec
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
 * If you need an aggregation that isn't wire-expressible (e.g. a `filter`-wrapped
 * stat), build a [StatGroup] / `*ListStats` directly with the vararg `Pair`
 * constructor - bypass the schema layer entirely.
 *
 * The schema-level [concurrency] is the deployment knob: every config materializes
 * via `config.materialize(concurrency)` inside the [StatGroup] / `*ListStats`
 * constructor.
 *
 * @sample com.eignex.kumulant.samples.schemaDeclarationAndRead
 */
@Suppress("AbstractClassCanBeConcreteClass") // concrete class would expose `register` and the modality
// helpers to direct callers; abstract enforces the "extend and declare stats" usage pattern.
abstract class StatSchema(val concurrency: Concurrency = Concurrency.None) : Schema<StatSpec>() {

    /** Pure-data, serializable view of this schema using kumulant's wire field `stats`. */
    fun statSchemaDef(): StatSchemaDef = StatSchemaDef(definition().entries)

    protected fun <R : Result> series(config: SeriesStatSpec<R>) = register(config) { StatKey<R>(it) }

    protected fun <R : Result> paired(config: PairedStatSpec<R>) = register(config) { StatKey<R>(it) }

    protected fun <R : Result> vector(config: VectorStatSpec<R>) = register(config) { StatKey<R>(it) }

    protected fun <R : Result> discrete(config: DiscreteStatSpec<R>) = register(config) { StatKey<R>(it) }

    /** Nest a sub-schema as a [GroupStatSpec]; materialization recurses at the parent's group construction. */
    protected fun <T : StatSchema> group(nestedSchema: T) =
        register(GroupStatSpec(nestedSchema.statSchemaDef().stats)) { GroupStatKey(it, nestedSchema) }
}

/** Series-modality specs from a schema, materialized at the schema's [concurrency][StatSchema.concurrency]. */
internal fun seriesSpecs(schema: StatSchema): List<BoundStat<*, out SeriesStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is SeriesStatSpec<*>) return@mapNotNull null
        toSpec<SeriesStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

/** Paired-modality specs from a schema. */
internal fun pairedSpecs(schema: StatSchema): List<BoundStat<*, out PairedStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is PairedStatSpec<*>) return@mapNotNull null
        toSpec<PairedStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

/** Vector-modality specs from a schema. */
internal fun vectorSpecs(schema: StatSchema): List<BoundStat<*, out VectorStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is VectorStatSpec<*>) return@mapNotNull null
        toSpec<VectorStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

/** Discrete-modality specs from a schema. */
internal fun discreteSpecs(schema: StatSchema): List<BoundStat<*, out DiscreteStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is DiscreteStatSpec<*>) return@mapNotNull null
        toSpec<DiscreteStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

/** Regression-modality specs from a schema. */
internal fun regressionSpecs(schema: StatSchema): List<BoundStat<*, out RegressionStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is RegressionStatSpec<*>) return@mapNotNull null
        toSpec<RegressionStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }

@Suppress("UNCHECKED_CAST")
internal fun <S : Stat<*>> toSpec(key: StatKey<*>, stat: S): BoundStat<*, out S, *> =
    BoundStat(key as StatKey<Result>, stat as Stat<Result>) as BoundStat<*, out S, *>

internal inline fun <reified S : Stat<*>> filterSpecs(specs: List<BoundStat<*, *, *>>): List<BoundStat<*, out S, *>> =
    specs.mapNotNull { (key, stat) -> if (stat is S) toSpec(key, stat) else null }

@Suppress("UNCHECKED_CAST")
internal fun mergeEntry(values: GroupResult, key: StatKey<*>, stat: Stat<*>) {
    val result = values.results[key.name] ?: return
    if (result is GroupResult && stat is GroupedStat) {
        stat.merge(result)
        return
    }
    (stat as Stat<Result>).merge(result)
}

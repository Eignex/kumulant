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
import com.eignex.kumulant.schema.spec.DiscreteStatSpec
import com.eignex.kumulant.schema.spec.GroupStatSpec
import com.eignex.kumulant.schema.spec.PairedStatSpec
import com.eignex.kumulant.schema.spec.RegressionStatSpec
import com.eignex.kumulant.schema.spec.SeriesStatSpec
import com.eignex.kumulant.schema.spec.StatSpec
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
 * If you need an aggregation that isn't wire-expressible (e.g. a `filter`-wrapped
 * stat), build a [StatGroup] / `*ListStats` directly with the vararg `Pair`
 * constructor - bypass the schema layer entirely.
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

    /**
     * Nest a sub-schema as a [GroupStatSpec]; materialization recurses at the parent's group construction.
     *
     * Series-only, and checked here rather than at materialization. [GroupStatSpec] is itself a
     * [SeriesStatSpec], so a nested schema holding another modality has no consistent home: the series
     * view of the parent rejects the child at materialize time, while the discrete or vector view skips
     * the whole subtree - reporting no error and no entries, and leaving the nested results unreachable
     * by any means. A mixed schema is legal at the root, where each modality's view picks its own
     * entries, so failing at the declaration is what tells the two cases apart.
     */
    protected fun <T : StatSchema> group(nestedSchema: T) =
        register(seriesOnlyGroupSpec(nestedSchema)) { GroupStatKey(it, nestedSchema) }
}

/**
 * [GroupStatSpec] over a nested schema, rejecting any entry that is not series.
 *
 * [GroupStatSpec] is itself a [SeriesStatSpec], so a nested schema holding another modality has no
 * consistent home: the series view of the parent rejects the child at materialize time, while the
 * discrete or vector view skips the whole subtree - reporting no error and no entries, and leaving the
 * nested results unreachable by any means. A mixed schema is legal at the root, where each modality's
 * view picks out its own entries, so failing at the declaration is what tells the two cases apart.
 */
private fun seriesOnlyGroupSpec(nestedSchema: StatSchema): GroupStatSpec {
    val nested = nestedSchema.statSchemaDef().stats
    for ((name, config) in nested) {
        require(config is SeriesStatSpec<*>) {
            "nested group entry '$name' is ${config::class.simpleName}; a nested schema must be series-only"
        }
    }
    return GroupStatSpec(nested)
}

/** Series-modality specs from a schema, materialized at [concurrency]. */
internal fun seriesSpecs(schema: StatSchema, concurrency: Concurrency): List<BoundStat<*, out SeriesStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is SeriesStatSpec<*>) return@mapNotNull null
        toSpec<SeriesStat<*>>(StatKey<Result>(name), config.materialize(concurrency))
    }

/** Paired-modality specs from a schema. */
internal fun pairedSpecs(schema: StatSchema, concurrency: Concurrency): List<BoundStat<*, out PairedStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is PairedStatSpec<*>) return@mapNotNull null
        toSpec<PairedStat<*>>(StatKey<Result>(name), config.materialize(concurrency))
    }

/** Vector-modality specs from a schema. */
internal fun vectorSpecs(schema: StatSchema, concurrency: Concurrency): List<BoundStat<*, out VectorStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is VectorStatSpec<*>) return@mapNotNull null
        toSpec<VectorStat<*>>(StatKey<Result>(name), config.materialize(concurrency))
    }

/** Discrete-modality specs from a schema. */
internal fun discreteSpecs(schema: StatSchema, concurrency: Concurrency): List<BoundStat<*, out DiscreteStat<*>, *>> =
    schema.entries.mapNotNull { (name, config) ->
        if (config !is DiscreteStatSpec<*>) return@mapNotNull null
        toSpec<DiscreteStat<*>>(StatKey<Result>(name), config.materialize(concurrency))
    }

/** Regression-modality specs from a schema. */
internal fun regressionSpecs(
    schema: StatSchema,
    concurrency: Concurrency,
): List<BoundStat<*, out RegressionStat<*>, *>> = schema.entries.mapNotNull { (name, config) ->
    if (config !is RegressionStatSpec<*>) return@mapNotNull null
    toSpec<RegressionStat<*>>(StatKey<Result>(name), config.materialize(concurrency))
}

@Suppress("UNCHECKED_CAST")
internal fun <S : Stat<*>> toSpec(key: StatKey<*>, stat: S): BoundStat<*, out S, *> =
    BoundStat(key as StatKey<Result>, stat as Stat<Result>) as BoundStat<*, out S, *>

@Suppress("UNCHECKED_CAST")
internal fun mergeEntry(values: GroupResult, key: StatKey<*>, stat: Stat<*>) {
    val result = values.results[key.name] ?: return
    if (result is GroupResult && stat is GroupedStat) {
        stat.merge(result)
        return
    }
    (stat as Stat<Result>).merge(result)
}

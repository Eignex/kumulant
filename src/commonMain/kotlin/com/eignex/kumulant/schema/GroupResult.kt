package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.skema.Schema
import com.eignex.skema.SchemaDef
import kotlinx.serialization.Serializable
import kotlin.properties.PropertyDelegateProvider
import kotlin.properties.ReadOnlyProperty

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
 * Subclass and declare stats via the [series], [paired], [vector], [discrete], and [group]
 * delegates; each property exposes a [StatKey] for typed retrieval from a [GroupResult].
 *
 * Two delegate forms exist for each modality:
 *  - **Config form**: `series(SumConfig)` — registers a serializable [StatConfig] in the
 *    inherited `entries` map. The schema is pure description; live materialization happens
 *    when a [StatGroup] (or paired/vector/discrete variant) is constructed from the schema.
 *  - **Live-stat form**: `series(Sum().withValue(1.0))` — back-door for stats whose
 *    configuration cannot be expressed as a [StatConfig] yet (e.g. operations like
 *    `withValue`/`withWeight`). Stored eagerly in [specs]; [definition] and
 *    [statSchemaDef] both throw if any entry was declared this way, since live entries
 *    cannot round-trip through the wire.
 *
 * The schema-level [concurrency] is the deployment knob: it propagates to every
 * registered stat at materialization time, both for the live form (via
 * `stat.create(concurrency)` at delegate time) and the config form (via
 * `config.materialize(concurrency)` inside the [StatGroup] constructor).
 */
abstract class StatSchema(val concurrency: Concurrency = Concurrency.None) : Schema<StatConfig>() {
    /**
     * Live entries declared via the live-stat overloads. Empty for fully
     * config-defined schemas. Configs go into the inherited [entries] map and
     * are materialized lazily by the schema-aware [StatGroup] constructors.
     */
    internal val specs = mutableListOf<StatSpec<*, *, *>>()

    /**
     * Pure-data, serializable view of this schema. Throws if any entry was
     * declared via the live-[Stat] delegate (without a [StatConfig]); switch
     * those entries to a config-taking overload to make the schema serializable.
     */
    override fun definition(): SchemaDef<StatConfig> {
        requireNoLiveEntries()
        return super.definition()
    }

    /**
     * Pure-data, serializable view of this schema using kumulant's wire field
     * `stats` (instead of skema's default `entries`). Routes through
     * [definition] so any [validate] override and the live-entry check both
     * run on the canonical wire path.
     */
    fun statSchemaDef(): StatSchemaDef = StatSchemaDef(definition().entries)

    private fun requireNoLiveEntries() {
        require(specs.isEmpty()) {
            val names = specs.map { it.key.name }
            "StatSchema cannot serialize: ${names.size} entries lack a StatConfig " +
                "(${names.joinToString(", ")}). Switch them to the config-taking delegate overload."
        }
    }

    protected fun <R : Result, S : SeriesStat<R>> series(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(toSpec(key, stat.create(concurrency)))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result> series(config: SeriesStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    protected fun <R : Result, S : PairedStat<R>> paired(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(toSpec(key, stat.create(concurrency)))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result> paired(config: PairedStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    protected fun <R : Result, S : VectorStat<R>> vector(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(toSpec(key, stat.create(concurrency)))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result> vector(config: VectorStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    protected fun <R : Result, S : DiscreteStat<R>> discrete(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(toSpec(key, stat.create(concurrency)))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result> discrete(config: DiscreteStatConfig<R>) =
        register(config) { StatKey<R>(it) }

    /**
     * Nest a sub-schema. If [nestedSchema] is fully config-defined, the entry
     * is captured on the wire as a [GroupStatConfig] and materialized lazily by
     * the parent's [StatGroup] constructor. Otherwise (nested has live-only
     * entries) a live [StatGroup] is built eagerly and stored in [specs] —
     * the parent can't be wire-serialized either, but it still works at runtime.
     */
    protected fun <T : StatSchema> group(nestedSchema: T, concurrency: Concurrency? = null) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, GroupStatKey<T>>> { _, property ->
            val key = GroupStatKey(property.name, nestedSchema)
            val nestedDef = runCatching { nestedSchema.statSchemaDef() }.getOrNull()
            if (nestedDef != null) {
                add(property.name, GroupStatConfig(nestedDef.stats))
            } else {
                specs.add(StatSpec(key, StatGroup(nestedSchema, concurrency)))
            }
            ReadOnlyProperty { _, _ -> key }
        }
}

/** All series-modality specs from a schema: configs materialized + live specs. */
internal fun seriesSpecs(schema: StatSchema): List<StatSpec<*, out SeriesStat<*>, *>> {
    val fromConfigs = schema.entries.mapNotNull { (name, config) ->
        if (config !is SeriesStatConfig<*>) return@mapNotNull null
        toSpec<SeriesStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }
    return fromConfigs + filterSpecs<SeriesStat<*>>(schema.specs)
}

/** All paired-modality specs from a schema: configs materialized + live specs. */
internal fun pairedSpecs(schema: StatSchema): List<StatSpec<*, out PairedStat<*>, *>> {
    val fromConfigs = schema.entries.mapNotNull { (name, config) ->
        if (config !is PairedStatConfig<*>) return@mapNotNull null
        toSpec<PairedStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }
    return fromConfigs + filterSpecs<PairedStat<*>>(schema.specs)
}

/** All vector-modality specs from a schema: configs materialized + live specs. */
internal fun vectorSpecs(schema: StatSchema): List<StatSpec<*, out VectorStat<*>, *>> {
    val fromConfigs = schema.entries.mapNotNull { (name, config) ->
        if (config !is VectorStatConfig<*>) return@mapNotNull null
        toSpec<VectorStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }
    return fromConfigs + filterSpecs<VectorStat<*>>(schema.specs)
}

/** All discrete-modality specs from a schema: configs materialized + live specs. */
internal fun discreteSpecs(schema: StatSchema): List<StatSpec<*, out DiscreteStat<*>, *>> {
    val fromConfigs = schema.entries.mapNotNull { (name, config) ->
        if (config !is DiscreteStatConfig<*>) return@mapNotNull null
        toSpec<DiscreteStat<*>>(StatKey<Result>(name), config.materialize(schema.concurrency))
    }
    return fromConfigs + filterSpecs<DiscreteStat<*>>(schema.specs)
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

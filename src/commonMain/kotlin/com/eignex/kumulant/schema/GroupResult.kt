package com.eignex.kumulant.schema

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.VectorStat
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
 * Declarative, typed schema for a group of stats.
 *
 * Subclass and declare stats via the [series], [paired], [vector], [discrete], and [group]
 * delegates; each property exposes a [StatKey] for typed retrieval from a [GroupResult].
 *
 * The schema-level [concurrency] propagates to every registered stat at delegate time
 * via `stat.create(concurrency)`. Set it on the schema to configure a coherent bag of
 * stats with one contract; per-stat `concurrency = …` arguments inside delegates are
 * overridden by the schema's choice.
 */
abstract class StatSchema(val concurrency: Concurrency = Concurrency.None) {
    internal val specs = mutableListOf<StatSpec<*, *, *>>()

    /**
     * Parallel pure-data record collected when delegates are given a [StatConfig].
     * Stays empty for entries built from a live [Stat], which is why [definition]
     * requires every entry to have provided a config — schemas that mix the two
     * paths can't be losslessly serialized.
     */
    private val def = mutableListOf<NamedStatConfig>()

    /**
     * Pure-data, serializable view of this schema. Throws if any entry was
     * declared via the live-[Stat] delegate (without a [StatConfig]); switch
     * those entries to a config-taking overload to make the schema serializable.
     */
    fun definition(): StatSchemaDef {
        require(def.size == specs.size) {
            val declared = def.map { it.name }.toSet()
            val missing = specs.map { it.key.name }.filter { it !in declared }
            "StatSchema cannot serialize: ${missing.size} entries lack a StatConfig " +
                "(${missing.joinToString(", ")}). Switch them to the config-taking delegate overload."
        }
        return StatSchemaDef(def.toList())
    }

    protected fun <R : Result, S : SeriesStat<R>> series(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat.create(concurrency)))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result> series(config: SeriesStatConfig<R>) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, config.materialize(concurrency)))
            def.add(NamedStatConfig(property.name, config))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result, S : PairedStat<R>> paired(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat.create(concurrency)))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result> paired(config: PairedStatConfig<R>) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, config.materialize(concurrency)))
            def.add(NamedStatConfig(property.name, config))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result, S : VectorStat<R>> vector(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat.create(concurrency)))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result> vector(config: VectorStatConfig<R>) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, config.materialize(concurrency)))
            def.add(NamedStatConfig(property.name, config))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result, S : DiscreteStat<R>> discrete(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat.create(concurrency)))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result> discrete(config: DiscreteStatConfig<R>) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, config.materialize(concurrency)))
            def.add(NamedStatConfig(property.name, config))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <T : StatSchema> group(nestedSchema: T, concurrency: Concurrency? = null) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, GroupStatKey<T>>> { _, property ->
            val key = GroupStatKey(property.name, nestedSchema)
            val groupStat = StatGroup(stats = filterSpecs<SeriesStat<*>>(nestedSchema.specs), concurrency = concurrency)

            specs.add(StatSpec(key, groupStat))
            // If the nested schema is fully config-defined, capture it on the wire too.
            runCatching { nestedSchema.definition() }.onSuccess { nestedDef ->
                def.add(NamedStatConfig(property.name, GroupStatConfig(nestedDef.stats)))
            }
            ReadOnlyProperty { _, _ -> key }
        }
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

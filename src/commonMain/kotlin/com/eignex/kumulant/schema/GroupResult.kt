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
     * Captured at construction time from [currentSkeletonMode]. When `true`,
     * the config-taking delegates skip building a live [Stat] — the schema
     * collects only [def] entries, leaving [specs] empty. Used by [bindTo] to
     * recover typed [StatKey]s from a class without paying for a parallel
     * unused live-stat set; the *actual* live group is built from the
     * wire-decoded [StatSchemaDef].
     */
    @PublishedApi internal val skeleton: Boolean = currentSkeletonMode

    /**
     * Pure-data, serializable view of this schema. Throws if any entry was
     * declared via the live-[Stat] delegate (without a [StatConfig]); switch
     * those entries to a config-taking overload to make the schema serializable.
     *
     * In [skeleton] mode the parity check is skipped — `specs` is
     * intentionally empty, and only entries that came through a config-taking
     * delegate populate `def`. A schema that uses live-stat overloads while
     * skeleton mode is on still fails here, because those entries appear in
     * `specs` but not `def`.
     */
    fun definition(): StatSchemaDef {
        if (!skeleton) {
            require(def.size == specs.size) {
                val declared = def.map { it.name }.toSet()
                val missing = specs.map { it.key.name }.filter { it !in declared }
                "StatSchema cannot serialize: ${missing.size} entries lack a StatConfig " +
                    "(${missing.joinToString(", ")}). Switch them to the config-taking delegate overload."
            }
        } else {
            require(specs.isEmpty()) {
                val missing = specs.map { it.key.name }
                "StatSchema cannot serialize: ${missing.size} entries used a live-stat delegate " +
                    "(${missing.joinToString(", ")}). Switch them to the config-taking delegate overload."
            }
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
            if (!skeleton) specs.add(StatSpec(key, config.materialize(concurrency)))
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
            if (!skeleton) specs.add(StatSpec(key, config.materialize(concurrency)))
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
            if (!skeleton) specs.add(StatSpec(key, config.materialize(concurrency)))
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
            if (!skeleton) specs.add(StatSpec(key, config.materialize(concurrency)))
            def.add(NamedStatConfig(property.name, config))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <T : StatSchema> group(nestedSchema: T, concurrency: Concurrency? = null) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, GroupStatKey<T>>> { _, property ->
            val key = GroupStatKey(property.name, nestedSchema)
            if (!skeleton) {
                val groupStat = StatGroup(stats = filterSpecs<SeriesStat<*>>(nestedSchema.specs), concurrency = concurrency)
                specs.add(StatSpec(key, groupStat))
            }
            // If the nested schema is fully config-defined, capture it on the wire too.
            runCatching { nestedSchema.definition() }.onSuccess { nestedDef ->
                def.add(NamedStatConfig(property.name, GroupStatConfig(nestedDef.stats)))
            }
            ReadOnlyProperty { _, _ -> key }
        }

    companion object {
        /**
         * When `true`, [StatSchema] subclasses constructed *during this call*
         * skip materializing live stats — they collect [def] entries only.
         * Set via [skeleton]; do not flip directly. Single-threaded (KMP-common
         * `var`). The realistic use case is startup-time hydration in
         * [StatSchemaDef.bindTo] where a parallel set of unused live stats
         * would otherwise be allocated and discarded.
         */
        @PublishedApi internal var currentSkeletonMode: Boolean = false

        /**
         * Run [factory] in skeleton mode — the returned schema has empty
         * [specs] but a fully populated [def] (and thus working [definition]
         * and typed [StatKey] properties). Reentrant; nested calls preserve
         * the prior flag value.
         */
        fun <T : StatSchema> skeleton(factory: () -> T): T {
            val prev = currentSkeletonMode
            currentSkeletonMode = true
            try {
                return factory()
            } finally {
                currentSkeletonMode = prev
            }
        }
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

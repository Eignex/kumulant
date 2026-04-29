package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.StreamMode
import kotlinx.serialization.Serializable
import kotlin.properties.PropertyDelegateProvider
import kotlin.properties.ReadOnlyProperty

/** Typed name identifying a result within a [GroupResult]. */
open class StatKey<R : Result>(val name: String)

/** Key for a nested group; [keys] exposes the sub-schema for dotted lookup. */
class GroupStatKey<K>(
    name: String,
    val keys: K
) : StatKey<GroupResult>(name)

/** Pairs a [StatKey] with the [Stat] that produces its result. */
data class StatSpec<
    R : Result,
    S : Stat<R>,
    K : StatKey<R>
    >(
    val key: K,
    val stat: S
)

/** Marker for stats whose result is a [GroupResult]. */
interface GroupedStat : Stat<GroupResult>

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

@Suppress("UNCHECKED_CAST")
private fun <S : Stat<*>> toSpec(key: StatKey<*>, stat: S): StatSpec<*, out S, *> =
    StatSpec(key as StatKey<Result>, stat as Stat<Result>) as StatSpec<*, out S, *>

private inline fun <reified S : Stat<*>> filterSpecs(
    specs: List<StatSpec<*, *, *>>
): List<StatSpec<*, out S, *>> =
    specs.mapNotNull { (key, stat) -> if (stat is S) toSpec(key, stat) else null }

@Suppress("UNCHECKED_CAST")
private fun mergeEntry(
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

/**
 * Declarative, typed schema for a group of stats.
 *
 * Subclass and declare stats via the [series], [paired], [vector], [discrete], and [group]
 * delegates; each property exposes a [StatKey] for typed retrieval from a [GroupResult].
 */
abstract class StatSchema {
    internal val specs = mutableListOf<StatSpec<*, *, *>>()

    protected fun <R : Result, S : SeriesStat<R>> series(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result, S : PairedStat<R>> paired(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result, S : VectorStat<R>> vector(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <R : Result, S : DiscreteStat<R>> discrete(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat))
            ReadOnlyProperty { _, _ -> key }
        }

    protected fun <T : StatSchema> group(nestedSchema: T, mode: StreamMode? = null) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, GroupStatKey<T>>> { _, property ->
            val key = GroupStatKey(property.name, nestedSchema)
            val groupStat = StatGroup(stats = filterSpecs<SeriesStat<*>>(nestedSchema.specs), mode = mode)

            specs.add(StatSpec(key, groupStat))
            ReadOnlyProperty { _, _ -> key }
        }
}

/**
 * Internal base shared by [StatGroup], [PairedStatGroup], and [VectorStatGroup]. Holds the
 * spec list and provides the modality-agnostic [read] / [merge] / [reset] implementations.
 */
sealed class AbstractStatGroup<S : Stat<*>>(
    protected val stats: List<StatSpec<*, out S, *>>,
    protected val mode: StreamMode?,
) : GroupedStat {
    final override fun read(timestampNanos: Long): GroupResult =
        GroupResult(stats.associate { (key, stat) -> key.name to stat.read(timestampNanos) })

    final override fun merge(values: GroupResult) {
        for ((key, stat) in stats) mergeEntry(values, key, stat)
    }

    final override fun reset() {
        for ((_, stat) in stats) stat.reset()
    }
}

/** Fans each update out to a heterogeneous list of [SeriesStat]s and reports their results keyed by name. */
class StatGroup(
    stats: List<StatSpec<*, out SeriesStat<*>, *>>,
    mode: StreamMode? = null,
) : AbstractStatGroup<SeriesStat<*>>(stats, mode), SeriesStat<GroupResult> {

    constructor(
        vararg stats: StatSpec<*, out SeriesStat<*>, *>,
        mode: StreamMode? = null
    ) : this(stats = stats.asList(), mode = mode)

    constructor(
        vararg stats: Pair<StatKey<*>, SeriesStat<*>>,
        mode: StreamMode? = null
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, mode = mode)

    constructor(schema: StatSchema, mode: StreamMode? = null) :
        this(stats = filterSpecs<SeriesStat<*>>(schema.specs), mode = mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(value, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): SeriesStat<GroupResult> {
        val effectiveMode = mode ?: this.mode
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveMode)) }
        return StatGroup(stats = newStats, mode = effectiveMode)
    }
}

/** [StatGroup] variant over paired (x, y) inputs. */
class PairedStatGroup(
    stats: List<StatSpec<*, out PairedStat<*>, *>>,
    mode: StreamMode? = null,
) : AbstractStatGroup<PairedStat<*>>(stats, mode), PairedStat<GroupResult> {

    constructor(
        vararg stats: StatSpec<*, out PairedStat<*>, *>,
        mode: StreamMode? = null
    ) : this(stats = stats.asList(), mode = mode)

    constructor(
        vararg stats: Pair<StatKey<*>, PairedStat<*>>,
        mode: StreamMode? = null
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, mode = mode)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(x, y, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): PairedStat<GroupResult> {
        val effectiveMode = mode ?: this.mode
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveMode)) }
        return PairedStatGroup(stats = newStats, mode = effectiveMode)
    }
}

/** [StatGroup] variant over vector inputs. */
class VectorStatGroup(
    stats: List<StatSpec<*, out VectorStat<*>, *>>,
    mode: StreamMode? = null,
) : AbstractStatGroup<VectorStat<*>>(stats, mode), VectorStat<GroupResult> {

    constructor(
        vararg stats: StatSpec<*, out VectorStat<*>, *>,
        mode: StreamMode? = null
    ) : this(stats = stats.asList(), mode = mode)

    constructor(
        vararg stats: Pair<StatKey<*>, VectorStat<*>>,
        mode: StreamMode? = null
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, mode = mode)

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(vector, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): VectorStat<GroupResult> {
        val effectiveMode = mode ?: this.mode
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveMode)) }
        return VectorStatGroup(stats = newStats, mode = effectiveMode)
    }
}

/** [StatGroup] variant over discrete (Long) inputs. */
class DiscreteStatGroup(
    stats: List<StatSpec<*, out DiscreteStat<*>, *>>,
    mode: StreamMode? = null,
) : AbstractStatGroup<DiscreteStat<*>>(stats, mode), DiscreteStat<GroupResult> {

    constructor(
        vararg stats: StatSpec<*, out DiscreteStat<*>, *>,
        mode: StreamMode? = null
    ) : this(stats = stats.asList(), mode = mode)

    constructor(
        vararg stats: Pair<StatKey<*>, DiscreteStat<*>>,
        mode: StreamMode? = null
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, mode = mode)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(value, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): DiscreteStat<GroupResult> {
        val effectiveMode = mode ?: this.mode
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveMode)) }
        return DiscreteStatGroup(stats = newStats, mode = effectiveMode)
    }
}

/** Builds a [StatSpec] from a string [name] and [value] stat. */
fun <R : Result, S : Stat<R>> stat(
    name: String,
    value: S
): StatSpec<R, S, StatKey<R>> = StatSpec(StatKey(name), value)

/** Builds a [StatSpec] from an existing [key] and [value] stat. */
fun <R : Result, S : Stat<R>, K : StatKey<R>> stat(
    key: K,
    value: S
): StatSpec<R, S, K> = StatSpec(key, value)

/** Builds a nested-group [StatSpec] whose [keys] sub-schema is passed to [build]. */
inline fun <K, S> group(
    name: String,
    keys: K,
    build: (K) -> S
): StatSpec<GroupResult, S, GroupStatKey<K>>
    where S : GroupedStat {
    val groupKey = GroupStatKey(name, keys)
    return StatSpec(groupKey, build(keys))
}

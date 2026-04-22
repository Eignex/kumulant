package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.StreamMode
import kotlinx.serialization.Serializable
import kotlin.properties.PropertyDelegateProvider
import kotlin.properties.ReadOnlyProperty

open class StatKey<R : Result>(val name: String)

class GroupStatKey<K>(
    name: String,
    val keys: K
) : StatKey<GroupResult>(name)

data class StatSpec<
    R : Result,
    S : Stat<R>,
    K : StatKey<R>
    >(
    val key: K,
    val stat: S
)

interface GroupedStat : Stat<GroupResult>

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
private fun toSeriesSpec(
    key: StatKey<*>,
    stat: SeriesStat<*>
): StatSpec<*, out SeriesStat<*>, *> {
    return StatSpec(key as StatKey<Result>, stat as SeriesStat<Result>)
}

private fun toSeriesSpecs(
    specs: List<StatSpec<*, *, *>>
): List<StatSpec<*, out SeriesStat<*>, *>> {
    return specs.mapNotNull { (key, stat) ->
        if (stat is SeriesStat<*>) {
            toSeriesSpec(key, stat)
        } else {
            null
        }
    }
}

@Suppress("UNCHECKED_CAST")
private fun toPairedSpec(
    key: StatKey<*>,
    stat: PairedStat<*>
): StatSpec<*, out PairedStat<*>, *> {
    return StatSpec(key as StatKey<Result>, stat as PairedStat<Result>)
}

@Suppress("UNCHECKED_CAST")
private fun toVectorSpec(
    key: StatKey<*>,
    stat: VectorStat<*>
): StatSpec<*, out VectorStat<*>, *> {
    return StatSpec(key as StatKey<Result>, stat as VectorStat<Result>)
}

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

abstract class StatSchema {
    internal val specs = mutableListOf<StatSpec<*, *, *>>()

    // 1. Fluent helper for SeriesStat
    protected fun <R : Result, S : SeriesStat<R>> stat(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat))
            ReadOnlyProperty { _, _ -> key }
        }

    // 2. Fluent helper for PairedStat
    protected fun <R : Result, S : PairedStat<R>> pairedStat(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat))
            ReadOnlyProperty { _, _ -> key }
        }

    // 3. Fluent helper for VectorStat
    protected fun <R : Result, S : VectorStat<R>> vectorStat(stat: S) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, StatKey<R>>> { _, property ->
            val key = StatKey<R>(property.name)
            specs.add(StatSpec(key, stat))
            ReadOnlyProperty { _, _ -> key }
        }

    // 4. Fluent helper for nested groups!
    protected fun <T : StatSchema> group(nestedSchema: T, mode: StreamMode? = null) =
        PropertyDelegateProvider<StatSchema, ReadOnlyProperty<StatSchema, GroupStatKey<T>>> { _, property ->
            val key = GroupStatKey(property.name, nestedSchema)
            val groupStat = StatGroup(stats = toSeriesSpecs(nestedSchema.specs), mode = mode)

            specs.add(StatSpec(key, groupStat))
            ReadOnlyProperty { _, _ -> key }
        }
}

class StatGroup(
    private val stats: List<StatSpec<*, out SeriesStat<*>, *>>,
    private val mode: StreamMode? = null
) : SeriesStat<GroupResult>, GroupedStat {

    constructor(
        vararg stats: StatSpec<*, out SeriesStat<*>, *>,
        mode: StreamMode? = null
    ) : this(
        stats = stats.asList(),
        mode = mode
    )

    constructor(
        vararg stats: Pair<StatKey<*>, SeriesStat<*>>,
        mode: StreamMode? = null
    ) : this(
        stats = stats.map { toSeriesSpec(it.first, it.second) },
        mode = mode
    )
    constructor(schema: StatSchema, mode: StreamMode? = null) : this(
        stats = toSeriesSpecs(schema.specs),
        mode = mode
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) {
            stat.update(value, timestampNanos, weight)
        }
    }

    override fun read(timestampNanos: Long): GroupResult {
        val map = stats.associate { (key, stat) ->
            key.name to stat.read(timestampNanos)
        }
        return GroupResult(map)
    }

    override fun merge(values: GroupResult) {
        for ((key, stat) in stats) {
            mergeEntry(values, key, stat)
        }
    }

    override fun reset() {
        for ((_, stat) in stats) {
            stat.reset()
        }
    }

    override fun create(mode: StreamMode?): SeriesStat<GroupResult> {
        val effectiveMode = mode ?: this.mode
        val newStats = stats.map { (key, stat) ->
            toSeriesSpec(key, stat.create(effectiveMode))
        }
        return StatGroup(stats = newStats, mode = effectiveMode)
    }
}

class PairedStatGroup(
    private val stats: List<StatSpec<*, out PairedStat<*>, *>>,
    private val mode: StreamMode? = null
) : PairedStat<GroupResult>, GroupedStat {

    constructor(
        vararg stats: StatSpec<*, out PairedStat<*>, *>,
        mode: StreamMode? = null
    ) : this(
        stats = stats.asList(),
        mode = mode
    )

    constructor(
        vararg stats: Pair<StatKey<*>, PairedStat<*>>,
        mode: StreamMode? = null
    ) : this(
        stats = stats.map { toPairedSpec(it.first, it.second) },
        mode = mode
    )

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) {
            stat.update(x, y, timestampNanos, weight)
        }
    }

    override fun read(timestampNanos: Long): GroupResult {
        val map = stats.associate { (key, stat) ->
            key.name to stat.read(timestampNanos)
        }
        return GroupResult(map)
    }

    override fun merge(values: GroupResult) {
        for ((key, stat) in stats) {
            mergeEntry(values, key, stat)
        }
    }

    override fun reset() {
        for ((_, stat) in stats) {
            stat.reset()
        }
    }

    override fun create(mode: StreamMode?): PairedStat<GroupResult> {
        val effectiveMode = mode ?: this.mode
        val newStats = stats.map { (key, stat) ->
            toPairedSpec(key, stat.create(effectiveMode))
        }
        return PairedStatGroup(stats = newStats, mode = effectiveMode)
    }
}

class VectorStatGroup(
    private val stats: List<StatSpec<*, out VectorStat<*>, *>>,
    private val mode: StreamMode? = null
) : VectorStat<GroupResult>, GroupedStat {

    constructor(
        vararg stats: StatSpec<*, out VectorStat<*>, *>,
        mode: StreamMode? = null
    ) : this(
        stats = stats.asList(),
        mode = mode
    )

    constructor(
        vararg stats: Pair<StatKey<*>, VectorStat<*>>,
        mode: StreamMode? = null
    ) : this(
        stats = stats.map { toVectorSpec(it.first, it.second) },
        mode = mode
    )

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) {
            stat.update(vector, timestampNanos, weight)
        }
    }

    override fun read(timestampNanos: Long): GroupResult {
        val map = stats.associate { (key, stat) ->
            key.name to stat.read(timestampNanos)
        }
        return GroupResult(map)
    }

    override fun merge(values: GroupResult) {
        for ((key, stat) in stats) {
            mergeEntry(values, key, stat)
        }
    }

    override fun reset() {
        for ((_, stat) in stats) {
            stat.reset()
        }
    }

    override fun create(mode: StreamMode?): VectorStat<GroupResult> {
        val effectiveMode = mode ?: this.mode
        val newStats = stats.map { (key, stat) ->
            toVectorSpec(key, stat.create(effectiveMode))
        }
        return VectorStatGroup(stats = newStats, mode = effectiveMode)
    }
}

fun <R : Result, S : Stat<R>> stat(
    name: String,
    value: S
): StatSpec<R, S, StatKey<R>> = StatSpec(StatKey(name), value)

fun <R : Result, S : Stat<R>, K : StatKey<R>> stat(
    key: K,
    value: S
): StatSpec<R, S, K> = StatSpec(key, value)

inline fun <K, S> group(
    name: String,
    keys: K,
    build: (K) -> S
): StatSpec<GroupResult, S, GroupStatKey<K>>
    where S : GroupedStat {
    val groupKey = GroupStatKey(name, keys)
    return StatSpec(groupKey, build(keys))
}

/**
 * Homogeneous, positional grouping: stats of the same result type composed into a
 * single [SeriesStat] whose result is a [ResultList]. Lighter than [StatGroup] when
 * named access is not needed — merge is by index, not by key.
 */
class ListStats<R : Result>(
    private val stats: List<SeriesStat<R>>,
    private val mode: StreamMode? = null,
) : SeriesStat<ResultList<R>> {

    constructor(vararg stats: SeriesStat<R>, mode: StreamMode? = null) :
        this(stats.asList(), mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        for (stat in stats) stat.update(value, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): ResultList<R> =
        ResultList(stats.map { it.read(timestampNanos) })

    override fun merge(values: ResultList<R>) {
        stats.zip(values.results).forEach { (stat, result) -> stat.merge(result) }
    }

    override fun reset() {
        for (stat in stats) stat.reset()
    }

    override fun create(mode: StreamMode?): SeriesStat<ResultList<R>> {
        val effectiveMode = mode ?: this.mode
        return ListStats(stats.map { it.create(effectiveMode) }, effectiveMode)
    }
}

class PairedListStats<R : Result>(
    private val stats: List<PairedStat<R>>,
    private val mode: StreamMode? = null,
) : PairedStat<ResultList<R>> {

    constructor(vararg stats: PairedStat<R>, mode: StreamMode? = null) :
        this(stats.asList(), mode)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        for (stat in stats) stat.update(x, y, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): ResultList<R> =
        ResultList(stats.map { it.read(timestampNanos) })

    override fun merge(values: ResultList<R>) {
        stats.zip(values.results).forEach { (stat, result) -> stat.merge(result) }
    }

    override fun reset() {
        for (stat in stats) stat.reset()
    }

    override fun create(mode: StreamMode?): PairedStat<ResultList<R>> {
        val effectiveMode = mode ?: this.mode
        return PairedListStats(stats.map { it.create(effectiveMode) }, effectiveMode)
    }
}

class VectorListStats<R : Result>(
    private val stats: List<VectorStat<R>>,
    private val mode: StreamMode? = null,
) : VectorStat<ResultList<R>> {

    constructor(vararg stats: VectorStat<R>, mode: StreamMode? = null) :
        this(stats.asList(), mode)

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        for (stat in stats) stat.update(vector, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): ResultList<R> =
        ResultList(stats.map { it.read(timestampNanos) })

    override fun merge(values: ResultList<R>) {
        stats.zip(values.results).forEach { (stat, result) -> stat.merge(result) }
    }

    override fun reset() {
        for (stat in stats) stat.reset()
    }

    override fun create(mode: StreamMode?): VectorStat<ResultList<R>> {
        val effectiveMode = mode ?: this.mode
        return VectorListStats(stats.map { it.create(effectiveMode) }, effectiveMode)
    }
}

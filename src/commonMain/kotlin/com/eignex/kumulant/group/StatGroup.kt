package com.eignex.kumulant.group

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode

/**
 * Internal base shared by [StatGroup], [PairedStatGroup], and [VectorStatGroup]. Holds the
 * spec list and provides the modality-agnostic [read] / [merge] / [reset] implementations.
 */
sealed class AbstractStatGroup<S : Stat<*>>(
    protected val stats: List<StatSpec<*, out S, *>>,
    protected val modeOverride: StreamMode?,
) : GroupedStat {
    final override val mode: StreamMode get() = modeOverride ?: defaultStreamMode

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
        val effectiveMode = mode ?: this.modeOverride
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
        val effectiveMode = mode ?: this.modeOverride
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
        val effectiveMode = mode ?: this.modeOverride
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
        val effectiveMode = mode ?: this.modeOverride
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveMode)) }
        return DiscreteStatGroup(stats = newStats, mode = effectiveMode)
    }
}

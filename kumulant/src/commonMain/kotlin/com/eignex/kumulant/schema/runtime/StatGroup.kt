package com.eignex.kumulant.schema.runtime

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.*
import com.eignex.kumulant.schema.spec.*

/**
 * Internal base shared by [StatGroup], [PairedStatGroup], and [VectorStatGroup]. Holds the
 * spec list and provides the modality-agnostic [read] / [merge] / [reset] implementations.
 */
sealed class AbstractStatGroup<S : Stat<*>>(
    protected val stats: List<BoundStat<*, out S, *>>,
    protected val concurrencyOverride: Concurrency?,
) : GroupedStat {
    final override val concurrency: Concurrency get() = concurrencyOverride ?: Concurrency.None

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
class StatGroup(stats: List<BoundStat<*, out SeriesStat<*>, *>>, concurrency: Concurrency? = null) :
    AbstractStatGroup<SeriesStat<*>>(stats, concurrency),
    SeriesStat<GroupResult> {

    constructor(
        vararg stats: BoundStat<*, out SeriesStat<*>, *>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.asList(), concurrency = concurrency)

    constructor(
        vararg stats: Pair<StatKey<*>, SeriesStat<*>>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, concurrency = concurrency)

    constructor(schema: StatSchema, concurrency: Concurrency? = null) :
        this(stats = seriesSpecs(schema), concurrency = concurrency)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<GroupResult> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveConcurrency)) }
        return StatGroup(stats = newStats, concurrency = effectiveConcurrency)
    }
}

/** [StatGroup] variant over paired (x, y) inputs. */
class PairedStatGroup(stats: List<BoundStat<*, out PairedStat<*>, *>>, concurrency: Concurrency? = null) :
    AbstractStatGroup<PairedStat<*>>(stats, concurrency),
    PairedStat<GroupResult> {

    constructor(
        vararg stats: BoundStat<*, out PairedStat<*>, *>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.asList(), concurrency = concurrency)

    constructor(
        vararg stats: Pair<StatKey<*>, PairedStat<*>>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, concurrency = concurrency)

    constructor(schema: StatSchema, concurrency: Concurrency? = null) :
        this(stats = pairedSpecs(schema), concurrency = concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(x, y, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<GroupResult> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveConcurrency)) }
        return PairedStatGroup(stats = newStats, concurrency = effectiveConcurrency)
    }
}

/** [StatGroup] variant over vector inputs. */
class VectorStatGroup(stats: List<BoundStat<*, out VectorStat<*>, *>>, concurrency: Concurrency? = null) :
    AbstractStatGroup<VectorStat<*>>(stats, concurrency),
    VectorStat<GroupResult> {

    constructor(
        vararg stats: BoundStat<*, out VectorStat<*>, *>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.asList(), concurrency = concurrency)

    constructor(
        vararg stats: Pair<StatKey<*>, VectorStat<*>>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, concurrency = concurrency)

    constructor(schema: StatSchema, concurrency: Concurrency? = null) :
        this(stats = vectorSpecs(schema), concurrency = concurrency)

    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(vector, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<GroupResult> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveConcurrency)) }
        return VectorStatGroup(stats = newStats, concurrency = effectiveConcurrency)
    }
}

/** [StatGroup] variant over discrete (Long) inputs. */
class DiscreteStatGroup(stats: List<BoundStat<*, out DiscreteStat<*>, *>>, concurrency: Concurrency? = null) :
    AbstractStatGroup<DiscreteStat<*>>(stats, concurrency),
    DiscreteStat<GroupResult> {

    constructor(
        vararg stats: BoundStat<*, out DiscreteStat<*>, *>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.asList(), concurrency = concurrency)

    constructor(
        vararg stats: Pair<StatKey<*>, DiscreteStat<*>>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, concurrency = concurrency)

    constructor(schema: StatSchema, concurrency: Concurrency? = null) :
        this(stats = discreteSpecs(schema), concurrency = concurrency)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): DiscreteStat<GroupResult> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveConcurrency)) }
        return DiscreteStatGroup(stats = newStats, concurrency = effectiveConcurrency)
    }
}

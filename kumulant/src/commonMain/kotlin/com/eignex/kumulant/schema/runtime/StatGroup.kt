package com.eignex.kumulant.schema.runtime

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RefusesMerge
import com.eignex.kumulant.core.RegressionStat
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
    init {
        // read() keys results by name, so a duplicate would silently drop one stat's result and then
        // feed the survivor's back into both on merge.
        requireUniqueNames(stats.map { (key, _) -> key.name to key }, this::class.simpleName ?: "StatGroup")
    }

    /**
     * The group's effective level: the one it was built with when a caller supplied it, otherwise the
     * *weakest* level any child uses.
     *
     * The two agree on the schema path, since a schema group builds every child at the level it was
     * handed. They can diverge only when a caller supplies stats it built itself, which is what the
     * vararg `Pair` constructor is for, and there the weakest child is the honest answer.
     *
     * Callers introspect this to decide whether reads need external synchronisation, so the weakest
     * child has to govern: the group can promise nothing its least-protected member does not. One
     * [Concurrency.None] child makes the whole group unsafe to share, and one [Concurrency.Relaxed]
     * child means a read can drift even if every sibling is exact.
     *
     * The declaration order of [Concurrency] happens to run weakest to strongest, so `minOfOrNull`
     * picks that child directly. [Concurrency.HighWrite] sorting last is not a claim that it is the
     * strongest guarantee; it is exactly as exact as [Concurrency.Strict] and falls back to it off the
     * JVM, so a mixed `Strict` / `HighWrite` group correctly reports `Strict`.
     *
     * Computed once: the children are fixed at construction, so there is no reason to walk them again
     * on every access.
     */
    final override val concurrency: Concurrency =
        concurrencyOverride ?: stats.minOfOrNull { (_, stat) -> stat.concurrency } ?: Concurrency.None

    final override fun read(timestampNanos: Long): GroupResult =
        GroupResult(stats.associate { (key, stat) -> key.name to stat.read(timestampNanos) })

    final override fun merge(values: GroupResult) {
        // Checked across every entry before any of them is touched. Merging in declaration order with no
        // way to undo one means a child that refuses partway leaves the group permanently inconsistent -
        // the entries ahead of it carrying both shards and the ones behind it carrying one - and a caller
        // that catches the exception around a shard roll-up has no way to tell. Throwing first also names
        // the key, instead of surfacing from whichever stat happened to be reached.
        for ((key, stat) in stats) {
            val refusal = (stat as? RefusesMerge)?.mergeRefusal ?: continue
            if (values.results[key.name] == null) continue
            throw UnsupportedOperationException("cannot merge group entry '${key.name}': $refusal")
        }
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

    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(stats = seriesSpecs(schema, concurrency), concurrency = concurrency)

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

    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(stats = pairedSpecs(schema, concurrency), concurrency = concurrency)

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

    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(stats = vectorSpecs(schema, concurrency), concurrency = concurrency)

    override fun update(vector: F64VectorLike, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(vector, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<GroupResult> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveConcurrency)) }
        return VectorStatGroup(stats = newStats, concurrency = effectiveConcurrency)
    }
}

/**
 * [StatGroup] variant over regression inputs, so a schema's `regression` entries have a home.
 *
 * Without it the declarator was reachable but unusable: every other group filters the schema by modality
 * with `mapNotNull`, so a regression entry was silently skipped and the failure surfaced only as a missing
 * key at read time.
 *
 * Every entry must expect the same feature width, since one update fans out to all of them.
 */
class RegressionStatGroup(stats: List<BoundStat<*, out RegressionStat<*>, *>>, concurrency: Concurrency? = null) :
    AbstractStatGroup<RegressionStat<*>>(stats, concurrency),
    RegressionStat<GroupResult> {

    constructor(
        vararg stats: BoundStat<*, out RegressionStat<*>, *>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.asList(), concurrency = concurrency)

    constructor(
        vararg stats: Pair<StatKey<*>, RegressionStat<*>>,
        concurrency: Concurrency? = null,
    ) : this(stats = stats.map { toSpec(it.first, it.second) }, concurrency = concurrency)

    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(stats = regressionSpecs(schema, concurrency), concurrency = concurrency)

    override val featureSize: Int = stats.firstOrNull()?.let { (_, stat) -> stat.featureSize }
        ?: error("RegressionStatGroup requires at least one entry")

    init {
        for ((key, stat) in stats) {
            require(stat.featureSize == featureSize) {
                "RegressionStatGroup entry '${key.name}' has featureSize=${stat.featureSize}, expected $featureSize"
            }
        }
    }

    override fun update(x: F64VectorLike, y: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(x, y, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): RegressionStat<GroupResult> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveConcurrency)) }
        return RegressionStatGroup(stats = newStats, concurrency = effectiveConcurrency)
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

    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(stats = discreteSpecs(schema, concurrency), concurrency = concurrency)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        for ((_, stat) in stats) stat.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): DiscreteStat<GroupResult> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        val newStats = stats.map { (key, stat) -> toSpec(key, stat.create(effectiveConcurrency)) }
        return DiscreteStatGroup(stats = newStats, concurrency = effectiveConcurrency)
    }
}

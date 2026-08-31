package com.eignex.kumulant.schema.runtime

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.*
import com.eignex.kumulant.schema.spec.*

internal fun requireUniqueNames(entries: List<Pair<String, *>>, typeName: String) {
    val duplicates = entries.map { it.first }.groupingBy { it }.eachCount().filter { it.value > 1 }.keys
    require(duplicates.isEmpty()) {
        "Duplicate stat names in $typeName: $duplicates - pass explicit Pair<String, ...> to disambiguate"
    }
}

/**
 * Internal base shared by [ListStats], [PairedListStats], and [VectorListStats]. Holds the
 * named entries and provides the modality-agnostic [read] / [merge] / [reset] implementations.
 */
internal sealed class AbstractListStats<R : Result, S : Stat<out R>>(
    protected val entries: List<Pair<String, S>>,
    protected val concurrencyOverride: Concurrency?,
    private val typeName: String,
) : Stat<ResultList<R>> {
    init {
        requireUniqueNames(entries, typeName)
    }

    /**
     * The weakest guarantee any entry offers, matching [AbstractStatGroup.concurrency].
     *
     * Callers introspect this to decide whether reads need external synchronisation, so
     * `StatGroup(schema)` and `ListStats(schema)` over the very same schema have to agree about what
     * they guarantee.
     *
     * Computed once rather than per access: the entries are fixed at construction.
     */
    final override val concurrency: Concurrency =
        concurrencyOverride ?: entries.minOfOrNull { (_, stat) -> stat.concurrency } ?: Concurrency.None

    final override fun read(timestampNanos: Long): ResultList<R> =
        ResultList(entries.map { it.first }, entries.map { it.second.read(timestampNanos) })

    final override fun reset() {
        for ((_, stat) in entries) stat.reset()
    }

    @Suppress("UNCHECKED_CAST")
    final override fun merge(values: ResultList<R>) {
        require(entries.size == values.results.size) {
            "$typeName merge size mismatch: expected ${entries.size}, got ${values.results.size}"
        }
        entries.zip(values.results).forEach { (pair, result) ->
            (pair.second as Stat<R>).merge(result)
        }
    }
}

/**
 * Heterogeneous, named-positional grouping composed into a single [SeriesStat]. The
 * result is a [ResultList] whose entries carry both position (for merge alignment) and
 * name (for `.toMap()`).
 *
 * Names default to each stat's `simpleName`; override with `Pair<String, SeriesStat>`
 * entries. Duplicate names throw at construction - disambiguate explicitly.
 *
 * Lighter than [StatGroup] when the [StatKey] / [BoundStat] apparatus isn't needed.
 */
internal class ListStats<R : Result>(entries: List<Pair<String, SeriesStat<out R>>>, concurrency: Concurrency? = null) :
    AbstractListStats<R, SeriesStat<out R>>(entries, concurrency, "ListStats"),
    SeriesStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, SeriesStat<out R>>, concurrency: Concurrency? = null) :
        this(entries.toList(), concurrency)

    @Suppress("UNCHECKED_CAST")
    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(
            entries = seriesSpecs(schema, concurrency).map { it.key.name to (it.stat as SeriesStat<out R>) },
            concurrency = concurrency,
        )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<ResultList<R>> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        return ListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveConcurrency) },
            effectiveConcurrency,
        )
    }
}

/** Auto-named [ListStats]: each stat keyed by its class `simpleName`. */
internal fun <R : Result> seriesListStats(
    vararg stats: SeriesStat<out R>,
    concurrency: Concurrency? = null,
): ListStats<R> = ListStats(stats.map { autoName(it) to it }, concurrency)

/** Paired-input counterpart of [ListStats]. */
internal class PairedListStats<R : Result>(
    entries: List<Pair<String, PairedStat<out R>>>,
    concurrency: Concurrency? = null,
) : AbstractListStats<R, PairedStat<out R>>(entries, concurrency, "PairedListStats"),
    PairedStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, PairedStat<out R>>, concurrency: Concurrency? = null) :
        this(entries.toList(), concurrency)

    @Suppress("UNCHECKED_CAST")
    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(
            entries = pairedSpecs(schema, concurrency).map { it.key.name to (it.stat as PairedStat<out R>) },
            concurrency = concurrency,
        )

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(x, y, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<ResultList<R>> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        return PairedListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveConcurrency) },
            effectiveConcurrency,
        )
    }
}

/** Auto-named [PairedListStats]: each stat keyed by its class `simpleName`. */
internal fun <R : Result> pairedListStats(
    vararg stats: PairedStat<out R>,
    concurrency: Concurrency? = null,
): PairedListStats<R> = PairedListStats(stats.map { autoName(it) to it }, concurrency)

/** Vector-input counterpart of [ListStats]. */
internal class VectorListStats<R : Result>(
    entries: List<Pair<String, VectorStat<out R>>>,
    concurrency: Concurrency? = null,
) : AbstractListStats<R, VectorStat<out R>>(entries, concurrency, "VectorListStats"),
    VectorStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, VectorStat<out R>>, concurrency: Concurrency? = null) :
        this(entries.toList(), concurrency)

    @Suppress("UNCHECKED_CAST")
    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(
            entries = vectorSpecs(schema, concurrency).map { it.key.name to (it.stat as VectorStat<out R>) },
            concurrency = concurrency,
        )

    override fun update(vector: F64VectorLike, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(vector, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<ResultList<R>> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        return VectorListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveConcurrency) },
            effectiveConcurrency,
        )
    }
}

/** Auto-named [VectorListStats]: each stat keyed by its class `simpleName`. */
internal fun <R : Result> vectorListStats(
    vararg stats: VectorStat<out R>,
    concurrency: Concurrency? = null,
): VectorListStats<R> = VectorListStats(stats.map { autoName(it) to it }, concurrency)

/** Discrete-input counterpart of [ListStats]. */
internal class DiscreteListStats<R : Result>(
    entries: List<Pair<String, DiscreteStat<out R>>>,
    concurrency: Concurrency? = null,
) : AbstractListStats<R, DiscreteStat<out R>>(entries, concurrency, "DiscreteListStats"),
    DiscreteStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, DiscreteStat<out R>>, concurrency: Concurrency? = null) :
        this(entries.toList(), concurrency)

    @Suppress("UNCHECKED_CAST")
    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(
            entries = discreteSpecs(schema, concurrency).map { it.key.name to (it.stat as DiscreteStat<out R>) },
            concurrency = concurrency,
        )

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): DiscreteStat<ResultList<R>> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        return DiscreteListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveConcurrency) },
            effectiveConcurrency,
        )
    }
}

/** Auto-named [DiscreteListStats]: each stat keyed by its class `simpleName`. */
internal fun <R : Result> discreteListStats(
    vararg stats: DiscreteStat<out R>,
    concurrency: Concurrency? = null,
): DiscreteListStats<R> = DiscreteListStats(stats.map { autoName(it) to it }, concurrency)

/** Regression-input counterpart of [ListStats]. Each `(x, y, ts, weight)` update fans
 *  out to every entry; the snapshot is a [ResultList] keyed by name. */
internal class RegressionListStats<R : Result>(
    entries: List<Pair<String, RegressionStat<out R>>>,
    concurrency: Concurrency? = null,
) : AbstractListStats<R, RegressionStat<out R>>(entries, concurrency, "RegressionListStats"),
    RegressionStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, RegressionStat<out R>>, concurrency: Concurrency? = null) :
        this(entries.toList(), concurrency)

    @Suppress("UNCHECKED_CAST")
    constructor(schema: StatSchema, concurrency: Concurrency = Concurrency.None) :
        this(
            entries = regressionSpecs(schema, concurrency).map {
                it.key.name to (it.stat as RegressionStat<out R>)
            },
            concurrency = concurrency,
        )

    override val featureSize: Int = entries.firstOrNull()?.second?.featureSize
        ?: error("RegressionListStats requires at least one entry")
    init {
        for ((name, stat) in entries) {
            require(stat.featureSize == featureSize) {
                "RegressionListStats entry '$name' has featureSize=${stat.featureSize}, expected $featureSize"
            }
        }
    }

    override fun update(x: F64VectorLike, y: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(x, y, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): RegressionStat<ResultList<R>> {
        val effectiveConcurrency = concurrency ?: this.concurrencyOverride
        return RegressionListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveConcurrency) },
            effectiveConcurrency,
        )
    }
}

private fun autoName(stat: Any): String = stat::class.simpleName ?: "Stat"

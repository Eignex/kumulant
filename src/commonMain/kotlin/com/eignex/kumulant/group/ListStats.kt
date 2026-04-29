package com.eignex.kumulant.group

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stream.StreamMode

private fun requireUniqueNames(entries: List<Pair<String, *>>, typeName: String) {
    val duplicates = entries.map { it.first }.groupingBy { it }.eachCount().filter { it.value > 1 }.keys
    require(duplicates.isEmpty()) {
        "Duplicate stat names in $typeName: $duplicates — pass explicit Pair<String, ...> to disambiguate"
    }
}

/**
 * Internal base shared by [ListStats], [PairedListStats], and [VectorListStats]. Holds the
 * named entries and provides the modality-agnostic [read] / [merge] / [reset] implementations.
 */
sealed class AbstractListStats<R : Result, S : Stat<out R>>(
    protected val entries: List<Pair<String, S>>,
    protected val mode: StreamMode?,
    private val typeName: String,
) : Stat<ResultList<R>> {
    init { requireUniqueNames(entries, typeName) }

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
 * entries. Duplicate names throw at construction — disambiguate explicitly.
 *
 * Lighter than [StatGroup] when the `StatKey` / `StatSpec` apparatus isn't needed.
 */
class ListStats<R : Result>(
    entries: List<Pair<String, SeriesStat<out R>>>,
    mode: StreamMode? = null,
) : AbstractListStats<R, SeriesStat<out R>>(entries, mode, "ListStats"),
    SeriesStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, SeriesStat<out R>>, mode: StreamMode? = null) :
        this(entries.toList(), mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(value, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): SeriesStat<ResultList<R>> {
        val effectiveMode = mode ?: this.mode
        return ListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveMode) },
            effectiveMode,
        )
    }
}

/** Auto-named [ListStats]: each stat keyed by its class `simpleName`. */
fun <R : Result> seriesListStats(
    vararg stats: SeriesStat<out R>,
    mode: StreamMode? = null,
): ListStats<R> = ListStats(stats.map { autoName(it) to it }, mode)

/** Paired-input counterpart of [ListStats]. */
class PairedListStats<R : Result>(
    entries: List<Pair<String, PairedStat<out R>>>,
    mode: StreamMode? = null,
) : AbstractListStats<R, PairedStat<out R>>(entries, mode, "PairedListStats"),
    PairedStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, PairedStat<out R>>, mode: StreamMode? = null) :
        this(entries.toList(), mode)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(x, y, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): PairedStat<ResultList<R>> {
        val effectiveMode = mode ?: this.mode
        return PairedListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveMode) },
            effectiveMode,
        )
    }
}

/** Auto-named [PairedListStats]: each stat keyed by its class `simpleName`. */
fun <R : Result> pairedListStats(
    vararg stats: PairedStat<out R>,
    mode: StreamMode? = null,
): PairedListStats<R> = PairedListStats(stats.map { autoName(it) to it }, mode)

/** Vector-input counterpart of [ListStats]. */
class VectorListStats<R : Result>(
    entries: List<Pair<String, VectorStat<out R>>>,
    mode: StreamMode? = null,
) : AbstractListStats<R, VectorStat<out R>>(entries, mode, "VectorListStats"),
    VectorStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, VectorStat<out R>>, mode: StreamMode? = null) :
        this(entries.toList(), mode)

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(vector, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): VectorStat<ResultList<R>> {
        val effectiveMode = mode ?: this.mode
        return VectorListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveMode) },
            effectiveMode,
        )
    }
}

/** Auto-named [VectorListStats]: each stat keyed by its class `simpleName`. */
fun <R : Result> vectorListStats(
    vararg stats: VectorStat<out R>,
    mode: StreamMode? = null,
): VectorListStats<R> = VectorListStats(stats.map { autoName(it) to it }, mode)

/** Discrete-input counterpart of [ListStats]. */
class DiscreteListStats<R : Result>(
    entries: List<Pair<String, DiscreteStat<out R>>>,
    mode: StreamMode? = null,
) : AbstractListStats<R, DiscreteStat<out R>>(entries, mode, "DiscreteListStats"),
    DiscreteStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, DiscreteStat<out R>>, mode: StreamMode? = null) :
        this(entries.toList(), mode)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(value, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): DiscreteStat<ResultList<R>> {
        val effectiveMode = mode ?: this.mode
        return DiscreteListStats(
            entries.map { (name, stat) -> name to stat.create(effectiveMode) },
            effectiveMode,
        )
    }
}

/** Auto-named [DiscreteListStats]: each stat keyed by its class `simpleName`. */
fun <R : Result> discreteListStats(
    vararg stats: DiscreteStat<out R>,
    mode: StreamMode? = null,
): DiscreteListStats<R> = DiscreteListStats(stats.map { autoName(it) to it }, mode)

private fun autoName(stat: Any): String = stat::class.simpleName ?: "Stat"

package com.eignex.kumulant.schema.runtime

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.combinedConcurrency

internal fun requireUniqueNames(entries: List<Pair<String, *>>, typeName: String) {
    val duplicates = entries.map { it.first }.groupingBy { it }.eachCount().filter { it.value > 1 }.keys
    require(duplicates.isEmpty()) {
        "Duplicate stat names in $typeName: $duplicates - pass explicit Pair<String, ...> to disambiguate"
    }
}

// Positions align merges; names identify result entries. Duplicate names fail at construction.
internal class ListStats<R : Result>(
    private val entries: List<Pair<String, SeriesStat<out R>>>,
    concurrency: Concurrency? = null,
) : SeriesStat<ResultList<R>> {

    constructor(vararg entries: Pair<String, SeriesStat<out R>>, concurrency: Concurrency? = null) :
        this(entries.toList(), concurrency)

    init {
        requireUniqueNames(entries, "ListStats")
    }

    private val concurrencyOverride: Concurrency? = concurrency

    override val concurrency: Concurrency =
        entries.map { (_, stat) -> stat.concurrency }.combinedConcurrency(concurrencyOverride ?: Concurrency.None)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        for ((_, stat) in entries) stat.update(value, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): ResultList<R> =
        ResultList(entries.map { it.first }, entries.map { it.second.read(timestampNanos) })

    override fun reset() {
        for ((_, stat) in entries) stat.reset()
    }

    override fun validateMerge(values: ResultList<R>) {
        require(entries.size == values.results.size) {
            "ListStats merge size mismatch: expected ${entries.size}, got ${values.results.size}"
        }
        for (i in entries.indices) {
            val (name, stat) = entries[i]
            validateMergeEntry(stat, values.results[i], name)
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun merge(values: ResultList<R>, timestampNanos: Long) {
        validateMerge(values)
        for (i in entries.indices) (entries[i].second as Stat<R>).merge(values.results[i], timestampNanos)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<ResultList<R>> = ListStats(
        entries.map { (name, stat) -> name to stat.create(concurrency ?: stat.concurrency) },
        concurrency ?: concurrencyOverride,
    )
}

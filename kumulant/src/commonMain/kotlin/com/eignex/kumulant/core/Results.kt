package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Marker for a snapshot returned by a [Stat]'s read/merge pipeline. */
interface Result

/**
 * Wraps an [inner] result with the coordinate [index] currently being evaluated.
 * Element-wise feedback wrappers (vector / regression / paired) pass this to the
 * projection AST so it can branch on `VIndex` and still address primary-snapshot
 * fields (`Center`, `Scale`, `Low`, `High`) via the transparent unwrap performed by
 * those AST nodes.
 */
data class IndexedResult(
    /** The per-coordinate primary snapshot. */
    val inner: Result,
    /** Index of the coordinate this snapshot belongs to (0-based). */
    val index: Int,
) : Result

/**
 * Ordered list of results with per-entry names. Produced by
 * [ListStats][com.eignex.kumulant.schema.runtime.ListStats] and the vector expansion helpers.
 *
 * Names disambiguate entries for map-style lookup while preserving positional order.
 * Constructing with duplicate names throws - pass explicit names to disambiguate.
 *
 * Positional producers (e.g. vector-expanded stats) use the secondary constructor
 * which auto-assigns index-based names ("0", "1", ...).
 */
@Serializable
@SerialName("ResultList")
data class ResultList<R : Result>(
    /** Per-entry names; same length as [results] and required to be unique. */
    val names: List<String>,
    /** Ordered list of per-entry snapshots. */
    val results: List<R>,
) : Result {
    init {
        require(names.size == results.size) {
            "names/results size mismatch: ${names.size} vs ${results.size}"
        }
        val duplicates = names.groupingBy { it }.eachCount().filter { it.value > 1 }.keys
        require(duplicates.isEmpty()) {
            "Duplicate names in ResultList: $duplicates"
        }
    }

    /** Positional constructor: auto-assigns index-based names ("0", "1", ...). */
    constructor(results: List<R>) : this(List(results.size) { it.toString() }, results)

    /** Returns a name-to-result mapping preserving entry order. */
    fun toMap(): Map<String, R> = names.zip(results).toMap()
}

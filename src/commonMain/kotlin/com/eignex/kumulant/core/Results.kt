package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
sealed interface Result

/**
 * Ordered list of results with per-entry names. Produced by `ListStats` and the vector
 * expansion helpers.
 *
 * Names disambiguate entries for map-style lookup while preserving positional order.
 * Constructing with duplicate names throws — pass explicit names to disambiguate.
 *
 * Positional producers (e.g. vector-expanded stats) use the secondary constructor
 * which auto-assigns index-based names ("0", "1", ...).
 */
@Serializable
@SerialName("List")
data class ResultList<R : Result>(
    val names: List<String>,
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

    fun toMap(): Map<String, R> = names.zip(results).toMap()
}

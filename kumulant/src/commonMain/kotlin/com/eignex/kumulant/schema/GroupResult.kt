package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Result
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Aggregated snapshot of a [com.eignex.kumulant.schema.runtime.StatGroup]: a map
 * from [StatKey.name] to the per-slot [Result]. Backs the result side of the
 * schema layer; use the `get(StatKey)` operators below for type-safe lookup
 * rather than going through [results] by string key.
 *
 * Nested groups produce nested [GroupResult]s; the [get] overloads taking a
 * [GroupStatKey] chain through one level at a time.
 *
 * `@Serializable` like every other [Result], so a `GroupResult` produced by
 * one process is the unit of merge consumed by another.
 */
@Serializable
@SerialName("GroupResult")
data class GroupResult(
    /** Per-stat snapshots keyed by [StatKey.name]. */
    val results: Map<String, Result>,
) : Result {
    /**
     * Typed lookup by [StatKey]. The phantom [R] on the key narrows the return
     * type so the caller doesn't cast. Throws if no result has been recorded
     * for that key; the error message lists the available keys so a typo is
     * obvious at the throw site.
     */
    @Suppress("UNCHECKED_CAST")
    operator fun <R : Result> get(key: StatKey<R>): R {
        val value = requireNotNull(results[key.name]) {
            "Result key '${key.name}' not found. Available: ${results.keys}"
        }
        return value as R
    }

    /** Typed lookup into a nested [GroupResult]: `parent[groupKey, innerKey]`. */
    operator fun <R : Result> get(group: StatKey<GroupResult>, key: StatKey<R>): R = this[group][key]

    /** Typed lookup of the nested [GroupResult] itself; first step of dotted access. */
    operator fun <K> get(group: GroupStatKey<K>): GroupResult = this[group as StatKey<GroupResult>]

    /** Typed lookup into a nested [GroupResult]: `parent[groupKey, innerKey]`. */
    operator fun <K, R : Result> get(group: GroupStatKey<K>, key: StatKey<R>): R = this[group][key]

    /**
     * Typed lookup into a nested [GroupResult] via a key-selector lambda.
     * Lets the caller write `result[outer.auth] { successes }` rather than
     * `result[outer.auth][outer.auth.keys.successes]`.
     */
    operator fun <K, R : Result> get(group: GroupStatKey<K>, select: K.() -> StatKey<R>): R =
        this[group][group.keys.select()]
}

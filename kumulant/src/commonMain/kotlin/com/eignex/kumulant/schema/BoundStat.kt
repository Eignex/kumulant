package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat

/**
 * Pairs a [StatKey] with the live [Stat] that produces results for that slot.
 * The combination is what a [StatGroup] / `ListStats` actually holds — the key
 * for typed lookup, the stat for accumulation. Carries three type parameters
 * so the key's [R] / [S] / [K] match without erasure shenanigans at the
 * call site:
 *
 * - [R] is the [Result] type the stat produces.
 * - [S] narrows the stat's modality ([com.eignex.kumulant.core.SeriesStat],
 *   [com.eignex.kumulant.core.PairedStat], etc.) so a group can refuse to
 *   accept mismatched modalities at construction.
 * - [K] is the key's own type ([StatKey] / [GroupStatKey]).
 *
 * Most callers don't construct [BoundStat] by hand — the [StatSchema]
 * declarators ([StatSchema.series], [StatSchema.paired], etc.) produce them.
 * Direct construction is useful when bypassing the schema for ad-hoc groups.
 */
data class BoundStat<
    R : Result,
    S : Stat<R>,
    K : StatKey<R>,
    >(
    /** Typed key under which [stat]'s result lives in the [GroupResult]. */
    val key: K,
    /** Live accumulator producing the result. */
    val stat: S,
)

/**
 * Marker interface for stats whose result is a [GroupResult]. Implemented by
 * [StatGroup] and its modality variants. Used in the [group] declarator below
 * to enforce that nested-group entries actually produce [GroupResult] rather
 * than some other [Result] shape.
 */
interface GroupedStat : Stat<GroupResult>

/**
 * Build a [BoundStat] from a string name and a live stat. Convenience for
 * ad-hoc groups: `BoundStat(StatKey(name), value)` with type inference.
 *
 * Prefer the [StatSchema] declarators when registering stats at schema
 * construction time — they produce typed [StatKey]s as delegate properties
 * for compile-time-safe lookup.
 */
fun <R : Result, S : Stat<R>> stat(name: String, value: S): BoundStat<R, S, StatKey<R>> =
    BoundStat(StatKey(name), value)

/**
 * Build a [BoundStat] from an existing [StatKey] and a live stat. Used when
 * the key was created elsewhere (e.g. via a [StatSchema] declarator) and you
 * want to pair it with a specific live instance — common in tests and in
 * materializer code paths.
 */
fun <R : Result, S : Stat<R>, K : StatKey<R>> stat(key: K, value: S): BoundStat<R, S, K> = BoundStat(key, value)

/**
 * Build a nested-group [BoundStat]. [build] is invoked with [keys] (the
 * sub-schema's key handle) and must return a [GroupedStat] — typically a
 * [StatGroup] constructed against that sub-schema. The resulting [BoundStat]
 * uses a [GroupStatKey] so dotted lookup `result[outerKey][innerKey]`
 * compiles.
 */
inline fun <K, S> group(
    name: String,
    keys: K,
    build: (K) -> S,
): BoundStat<GroupResult, S, GroupStatKey<K>>
    where S : GroupedStat {
    val groupKey = GroupStatKey(name, keys)
    return BoundStat(groupKey, build(keys))
}

package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat

/** Pairs a [StatKey] with the [Stat] that produces its result. */
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

/** Marker for stats whose result is a [GroupResult]. */
interface GroupedStat : Stat<GroupResult>

/** Builds a [BoundStat] from a string [name] and [value] stat. */
fun <R : Result, S : Stat<R>> stat(name: String, value: S): BoundStat<R, S, StatKey<R>> =
    BoundStat(StatKey(name), value)

/** Builds a [BoundStat] from an existing [key] and [value] stat. */
fun <R : Result, S : Stat<R>, K : StatKey<R>> stat(key: K, value: S): BoundStat<R, S, K> = BoundStat(key, value)

/** Builds a nested-group [BoundStat] whose [keys] sub-schema is passed to [build]. */
inline fun <K, S> group(
    name: String,
    keys: K,
    build: (K) -> S,
): BoundStat<GroupResult, S, GroupStatKey<K>>
    where S : GroupedStat {
    val groupKey = GroupStatKey(name, keys)
    return BoundStat(groupKey, build(keys))
}

package com.eignex.kumulant.group

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat

/** Pairs a [StatKey] with the [Stat] that produces its result. */
data class StatSpec<
    R : Result,
    S : Stat<R>,
    K : StatKey<R>
    >(
    val key: K,
    val stat: S
)

/** Marker for stats whose result is a [GroupResult]. */
interface GroupedStat : Stat<GroupResult>

/** Builds a [StatSpec] from a string [name] and [value] stat. */
fun <R : Result, S : Stat<R>> stat(
    name: String,
    value: S
): StatSpec<R, S, StatKey<R>> = StatSpec(StatKey(name), value)

/** Builds a [StatSpec] from an existing [key] and [value] stat. */
fun <R : Result, S : Stat<R>, K : StatKey<R>> stat(
    key: K,
    value: S
): StatSpec<R, S, K> = StatSpec(key, value)

/** Builds a nested-group [StatSpec] whose [keys] sub-schema is passed to [build]. */
inline fun <K, S> group(
    name: String,
    keys: K,
    build: (K) -> S
): StatSpec<GroupResult, S, GroupStatKey<K>>
    where S : GroupedStat {
    val groupKey = GroupStatKey(name, keys)
    return StatSpec(groupKey, build(keys))
}

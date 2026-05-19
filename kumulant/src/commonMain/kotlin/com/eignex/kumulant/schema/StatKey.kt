package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Result

/** Typed name identifying a result within a [GroupResult]. */
open class StatKey<R : Result>(
    /** Wire name used by the underlying [com.eignex.skema.Schema]. */
    val name: String,
)

/** Key for a nested group; [keys] exposes the sub-schema for dotted lookup. */
class GroupStatKey<K>(
    name: String,
    /** Typed handle to the nested schema's keys, for `group[group.keys.foo]` lookups. */
    val keys: K,
) : StatKey<GroupResult>(name)

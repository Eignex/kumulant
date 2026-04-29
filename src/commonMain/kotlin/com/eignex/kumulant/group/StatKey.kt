package com.eignex.kumulant.group

import com.eignex.kumulant.core.Result

/** Typed name identifying a result within a [GroupResult]. */
open class StatKey<R : Result>(val name: String)

/** Key for a nested group; [keys] exposes the sub-schema for dotted lookup. */
class GroupStatKey<K>(
    name: String,
    val keys: K
) : StatKey<GroupResult>(name)

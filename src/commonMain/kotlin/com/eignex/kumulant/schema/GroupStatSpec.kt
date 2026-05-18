package com.eignex.kumulant.schema

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Serializable spec for a nested series-modality [StatGroup]. Holds a
 * recursive map of [StatSpec] entries keyed by name; every entry must itself
 * be a [SeriesStatSpec]; materialization happens in `StatFactory.kt`.
 */
@Serializable
@SerialName("GroupStatSpec")
data class GroupStatSpec(
    /** Nested specs keyed by [StatKey.name]; each must be a [SeriesStatSpec]. */
    val stats: Map<String, StatSpec>,
) : SeriesStatSpec<GroupResult>

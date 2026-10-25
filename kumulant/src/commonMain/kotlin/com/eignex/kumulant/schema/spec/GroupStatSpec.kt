package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.schema.GroupResult
import com.eignex.kumulant.schema.StatKey
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Serializable spec for a nested series-modality
 * [com.eignex.kumulant.schema.runtime.StatGroup]. Holds a
 * recursive map of [StatSpec] entries keyed by name; every entry must itself
 * be a [SeriesStatSpec]; materialization happens in `StatFactory.kt`.
 */
@Serializable
@SerialName("GroupStatSpec")
data class GroupStatSpec(
    /** Nested specs keyed by [StatKey.name]; each must be a [SeriesStatSpec]. */
    val stats: Map<String, StatSpec>,
) : SeriesStatSpec<GroupResult>

/** Serializable recipe for a nested [com.eignex.kumulant.schema.runtime.PairedStatGroup]. */
@Serializable
@SerialName("PairedGroupStatSpec")
data class PairedGroupStatSpec(
    /** Nested specs keyed by name; each must be a [PairedStatSpec]. */
    val stats: Map<String, StatSpec>,
) : PairedStatSpec<GroupResult>

/** Serializable recipe for a nested [com.eignex.kumulant.schema.runtime.VectorStatGroup]. */
@Serializable
@SerialName("VectorGroupStatSpec")
data class VectorGroupStatSpec(
    /** Nested specs keyed by name; each must be a [VectorStatSpec]. */
    val stats: Map<String, StatSpec>,
) : VectorStatSpec<GroupResult>

/** Serializable recipe for a nested [com.eignex.kumulant.schema.runtime.DiscreteStatGroup]. */
@Serializable
@SerialName("DiscreteGroupStatSpec")
data class DiscreteGroupStatSpec(
    /** Nested specs keyed by name; each must be a [DiscreteStatSpec]. */
    val stats: Map<String, StatSpec>,
) : DiscreteStatSpec<GroupResult>

/** Serializable recipe for a nested [com.eignex.kumulant.schema.runtime.RegressionStatGroup]. */
@Serializable
@SerialName("RegressionGroupStatSpec")
data class RegressionGroupStatSpec(
    /** Nested specs keyed by name; each must be a [RegressionStatSpec]. */
    val stats: Map<String, StatSpec>,
) : RegressionStatSpec<GroupResult>

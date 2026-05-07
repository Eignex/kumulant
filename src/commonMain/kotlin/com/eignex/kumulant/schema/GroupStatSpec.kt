package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Serializable spec for a nested series-modality [StatGroup]. Holds a
 * recursive map of [StatSpec] entries keyed by name; every entry must itself
 * be a [SeriesStatSpec].
 */
@Serializable
@SerialName("GroupStatSpec")
data class GroupStatSpec(val stats: Map<String, StatSpec>) : SeriesStatSpec<GroupResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<GroupResult> {
        val materialized = stats.map { (name, config) ->
            require(config is SeriesStatSpec<*>) {
                "GroupStatSpec entry '$name' has config ${config::class.simpleName}, " +
                    "expected a SeriesStatSpec"
            }
            toSpec(StatKey<com.eignex.kumulant.core.Result>(name), config.materialize(concurrency))
        }
        return StatGroup(stats = materialized, concurrency = concurrency)
    }
}

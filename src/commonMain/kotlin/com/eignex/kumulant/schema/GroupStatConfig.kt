package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Serializable configuration for a nested series-modality [StatGroup]. Holds a
 * recursive list of [NamedStatConfig] entries; every entry must itself be a
 * [SeriesStatConfig].
 *
 * On the wire:
 * ```yaml
 * $type: GroupStatConfig
 * stats:
 *   - name: requests
 *     config: { $type: SumConfig }
 * ```
 */
@Serializable
@SerialName("GroupStatConfig")
data class GroupStatConfig(val stats: List<NamedStatConfig>) : SeriesStatConfig<GroupResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<GroupResult> {
        val materialized = stats.map { (name, config) ->
            require(config is SeriesStatConfig<*>) {
                "GroupStatConfig entry '$name' has config ${config::class.simpleName}, " +
                    "expected a SeriesStatConfig"
            }
            toSpec(StatKey<com.eignex.kumulant.core.Result>(name), config.materialize(concurrency))
        }
        return StatGroup(stats = materialized, concurrency = concurrency)
    }
}

package com.eignex.kumulant.schema

import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.Result
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Center plus a configurable multiple of scale, derived from any [HasCenterScale]
 * result via the `BandSeries` spec. The band wrapper forwards `update` / `reset` /
 * `create` to the inner stat and projects `read` into this [BandResult].
 */
@Serializable
@SerialName("BandResult")
data class BandResult(
    /** Center exposed by the inner result. */
    val center: Double,
    /** Scale exposed by the inner result. */
    val scale: Double,
    /** Multiplier applied to [scale] when computing [lower] and [upper]. */
    val k: Double,
    /** `center - k * scale`. */
    val lower: Double,
    /** `center + k * scale`. */
    val upper: Double,
) : Result

package com.eignex.kumulant.schema.decay

import com.eignex.kumulant.stat.decay.DecayWeighting
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration.Companion.milliseconds

/**
 * Wire-friendly counterpart of [DecayWeighting]. The two strategies are split
 * by field type rather than discriminated union so each decay-stat spec can
 * statically constrain itself to the right strategy (e.g. [com.eignex.kumulant.schema.spec.DecayingSum]
 * only accepts [HalfLife]).
 *
 * Wall-clock durations travel as `Long` milliseconds rather than
 * `kotlin.time.Duration` to avoid the experimental `Duration` serializer and
 * keep the wire compact.
 */
@Serializable
sealed interface DecayWeightingSpec

/** Wall-clock half-life decay: weight halves every [durationMillis]. */
@Serializable
@SerialName("HalfLife")
data class HalfLife(
    /** Half-life in milliseconds. */
    val durationMillis: Long,
) : DecayWeightingSpec {
    /** Inflate to the runtime [DecayWeighting.HalfLife] form. */
    fun toDecayWeighting(): DecayWeighting.HalfLife = DecayWeighting.HalfLife(durationMillis.milliseconds)
}

/** Per-observation decay: each new sample carries weight [alpha] against the running estimate. */
@Serializable
@SerialName("Alpha")
data class Alpha(
    /** Smoothing factor in `(0, 1]`; larger = more weight on recent samples. */
    val alpha: Double,
) : DecayWeightingSpec {
    /** Inflate to the runtime [DecayWeighting.Alpha] form. */
    fun toDecayWeighting(): DecayWeighting.Alpha = DecayWeighting.Alpha(alpha)
}

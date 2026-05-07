package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import kotlinx.serialization.Serializable

/**
 * Pure-data configuration for a [Stat]. Each variant is a `data class` (or
 * `data object` for parameter-less stats) whose fields are exactly the stat's
 * configuration surface — no live cells, no locks, no [Concurrency].
 *
 * The wire form uses `$type` as the polymorphic discriminator and the simple
 * Kotlin class name as its value (see [SchemaJson]). Defaults on each Config
 * variant match the corresponding stat's constructor defaults so authored
 * YAML stays terse under `encodeDefaults = false`.
 *
 * `Concurrency` is *not* on the wire — it's a deployment knob passed to
 * [materialize] when the schema is rehydrated.
 */
@Serializable
sealed interface StatConfig {
    fun materialize(concurrency: Concurrency = Concurrency.None): Stat<*>
}

/** [StatConfig] that materializes into a [SeriesStat] with result type [R]. */
@Serializable
sealed interface SeriesStatConfig<R : Result> : StatConfig {
    override fun materialize(concurrency: Concurrency): SeriesStat<R>
}

/** [StatConfig] that materializes into a [PairedStat] with result type [R]. */
@Serializable
sealed interface PairedStatConfig<R : Result> : StatConfig {
    override fun materialize(concurrency: Concurrency): PairedStat<R>
}

/** [StatConfig] that materializes into a [VectorStat] with result type [R]. */
@Serializable
sealed interface VectorStatConfig<R : Result> : StatConfig {
    override fun materialize(concurrency: Concurrency): VectorStat<R>
}

/** [StatConfig] that materializes into a [DiscreteStat] with result type [R]. */
@Serializable
sealed interface DiscreteStatConfig<R : Result> : StatConfig {
    override fun materialize(concurrency: Concurrency): DiscreteStat<R>
}

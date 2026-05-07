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
 * Pure-data recipe for a [Stat]. Each variant is a `data class` (or
 * `data object` for parameter-less stats) whose fields are exactly the stat's
 * configuration surface — no live cells, no locks, no [Concurrency].
 *
 * The wire form uses `$type` as the polymorphic discriminator and the simple
 * Kotlin class name as its value (see [SchemaJson]). Defaults on each spec
 * match the corresponding stat's constructor defaults so authored JSON stays
 * terse under `encodeDefaults = false`.
 *
 * `Concurrency` is *not* on the wire — it's a deployment knob passed to
 * [materialize] when the schema is rehydrated.
 */
@Serializable
sealed interface StatSpec {
    fun materialize(concurrency: Concurrency = Concurrency.None): Stat<*>
}

/** [StatSpec] that materializes into a [SeriesStat] with result type [R]. */
@Serializable
sealed interface SeriesStatSpec<R : Result> : StatSpec {
    override fun materialize(concurrency: Concurrency): SeriesStat<R>
}

/** [StatSpec] that materializes into a [PairedStat] with result type [R]. */
@Serializable
sealed interface PairedStatSpec<R : Result> : StatSpec {
    override fun materialize(concurrency: Concurrency): PairedStat<R>
}

/** [StatSpec] that materializes into a [VectorStat] with result type [R]. */
@Serializable
sealed interface VectorStatSpec<R : Result> : StatSpec {
    override fun materialize(concurrency: Concurrency): VectorStat<R>
}

/** [StatSpec] that materializes into a [DiscreteStat] with result type [R]. */
@Serializable
sealed interface DiscreteStatSpec<R : Result> : StatSpec {
    override fun materialize(concurrency: Concurrency): DiscreteStat<R>
}

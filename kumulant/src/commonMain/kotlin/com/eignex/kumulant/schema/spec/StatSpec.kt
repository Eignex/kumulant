package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.core.Result
import kotlinx.serialization.Serializable

/**
 * Pure-data recipe for a [com.eignex.kumulant.core.Stat]. Each variant is a `data class`
 * (or `data object` for parameter-less stats) whose fields are exactly the stat's
 * configuration surface - no live cells, no locks, no
 * [com.eignex.kumulant.core.Concurrency].
 *
 * Polymorphism is by `@SerialName` on each variant, so any
 * [kotlinx.serialization][kotlinx.serialization.KSerializer] format that
 * supports open polymorphism works. Defaults on each spec match the
 * corresponding stat's constructor defaults so encoded payloads stay terse
 * when the format is configured with `encodeDefaults = false`.
 *
 * Construction of the live stat happens externally via the `materialize`
 * extension functions in `StatFactory.kt` - one `when` per modality,
 * one cast at the boundary. Specs stay as pure data so the wire format
 * doesn't drag behaviour through it. [com.eignex.kumulant.core.Concurrency] is *not*
 * on the wire - it's a deployment knob passed in at materialize time.
 *
 * The generic `<R>` on the modality-specific spec interfaces is a phantom
 * marker: it carries the result type through the schema declarators
 * (`series(spec): StatKey<R>`) and the materialize return type, but it
 * does not appear in the wire format.
 *
 * @sample com.eignex.kumulant.samples.specRoundTrip
 */
@Serializable
sealed interface StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.SeriesStat] with result type [R]. */
@Serializable
sealed interface SeriesStatSpec<R : Result> : StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.PairedStat] with result type [R]. */
@Serializable
sealed interface PairedStatSpec<R : Result> : StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.VectorStat] with result type [R]. */
@Serializable
sealed interface VectorStatSpec<R : Result> : StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.DiscreteStat] with result type [R]. */
@Serializable
sealed interface DiscreteStatSpec<R : Result> : StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.RegressionStat] with result type [R]. */
@Serializable
sealed interface RegressionStatSpec<R : Result> : StatSpec

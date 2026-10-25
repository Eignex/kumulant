package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.serialization.DiscreteStatSpecSerializer
import com.eignex.kumulant.schema.serialization.PairedStatSpecSerializer
import com.eignex.kumulant.schema.serialization.RegressionStatSpecSerializer
import com.eignex.kumulant.schema.serialization.SeriesStatSpecSerializer
import com.eignex.kumulant.schema.serialization.StatSpecSerializer
import com.eignex.kumulant.schema.serialization.VectorStatSpecSerializer
import kotlinx.serialization.Serializable

/**
 * Pure-data recipe for a [com.eignex.kumulant.core.Stat]. Each variant is a `data class`
 * (or `data object` for parameter-less stats) whose fields are exactly the stat's
 * configuration surface - no live cells, no locks, no
 * [com.eignex.kumulant.core.Concurrency].
 *
 * JSON uses names for parameterless nodes, such as `"mean"`, and operator keys
 * for configured nodes, such as `{"ewma":{"alpha":0.1}}`. Decorators retain named
 * fields, such as `{"transform":{"expr":{"add":["x",1]},"inner":"mean"}}`.
 * Serialize through this root or a modality interface to preserve that grammar.
 * Other serialization formats retain the generated sealed-class representation.
 * Defaults match the corresponding stat constructors and follow the format's
 * `encodeDefaults` setting.
 *
 * Construction of the live stat happens externally via the `materialize`
 * extension functions. Each specification tree is compiled into a construction plan that checks
 * modalities and result capabilities before allocating accumulators, with one cast at the typed
 * materialization boundary. Specs stay as pure data so the wire format
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
@Serializable(with = StatSpecSerializer::class)
sealed interface StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.SeriesStat] with result type [R]. */
@Serializable(with = SeriesStatSpecSerializer::class)
sealed interface SeriesStatSpec<R : Result> : StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.PairedStat] with result type [R]. */
@Serializable(with = PairedStatSpecSerializer::class)
sealed interface PairedStatSpec<R : Result> : StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.VectorStat] with result type [R]. */
@Serializable(with = VectorStatSpecSerializer::class)
sealed interface VectorStatSpec<R : Result> : StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.DiscreteStat] with result type [R]. */
@Serializable(with = DiscreteStatSpecSerializer::class)
sealed interface DiscreteStatSpec<R : Result> : StatSpec

/** [StatSpec] that materializes into a [com.eignex.kumulant.core.RegressionStat] with result type [R]. */
@Serializable(with = RegressionStatSpecSerializer::class)
sealed interface RegressionStatSpec<R : Result> : StatSpec

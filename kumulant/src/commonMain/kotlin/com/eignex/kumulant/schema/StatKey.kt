package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Result

/**
 * Typed handle to one entry in a [StatSchema] / [GroupResult]. Carries the
 * result type [R] as a phantom type so reading back from a [GroupResult]
 * produces a typed value rather than an `Any`:
 *
 * ```kotlin
 * object Telemetry : StatSchema() {
 *     val latency by series(Mean)
 * }
 * val result: WeightedMeanResult = group.read()[Telemetry.latency]
 * ```
 *
 * Keys come out of the schema's declarators (`series`, `paired`, `vector`,
 * `discrete`, `group`); callers don't usually construct them by hand.
 *
 * The [R] type parameter is a phantom marker — narrows the return type of
 * `GroupResult.get(key)` without affecting the wire format.
 */
open class StatKey<R : Result>(
    /** Wire-side name registered on the underlying [com.eignex.skema.Schema]. */
    val name: String,
)

/**
 * Specialised [StatKey] for nested-group slots. The [keys] field exposes the
 * nested schema's own [StatKey]s by reference, so dotted lookup composes:
 *
 * ```kotlin
 * object Outer : StatSchema() {
 *     val auth by group(object : StatSchema() {
 *         val successes by series(Sum)
 *     })
 * }
 * val total: SumResult = result[Outer.auth][Outer.auth.keys.successes]
 * ```
 */
class GroupStatKey<K>(
    name: String,
    /** Typed handle to the nested schema's keys, for `group[group.keys.foo]` lookups. */
    val keys: K,
) : StatKey<GroupResult>(name)

package com.eignex.kumulant.schema.runtime

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.*
import com.eignex.kumulant.schema.spec.*
import kotlinx.serialization.Serializable

/**
 * Pure-data, serializable form of a [StatSchema]. The wire field is `stats`
 * (kumulant convention, customised over skema's default `entries`). Decode an
 * incoming wire payload into this type and rehydrate a live group via
 * [materializeSeries] (or one of the modality-specific variants).
 */
@Serializable
data class StatSchemaDef(
    /** Per-entry specs keyed by [StatKey.name]. */
    val stats: Map<String, StatSpec>,
)

/** Materialize every entry, regardless of modality. Caller filters by stat type. */
fun StatSchemaDef.materialize(concurrency: Concurrency = Concurrency.None): List<BoundStat<*, *, *>> =
    stats.map { (name, config) -> name to config.compile(name) }
        .map { (name, plan) -> bind(name, plan.build(concurrency)) }

/** Materialize series-modality entries only; throws if any entry isn't series. */
fun StatSchemaDef.materializeSeries(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out SeriesStat<*>, *>> = materializeAs(concurrency, SeriesStatSpec<*>::compileSeries)

/** Materialize paired-modality entries only; throws if any entry isn't paired. */
fun StatSchemaDef.materializePaired(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out PairedStat<*>, *>> = materializeAs(concurrency, PairedStatSpec<*>::compilePaired)

/** Materialize vector-modality entries only; throws if any entry isn't vector. */
fun StatSchemaDef.materializeVector(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out VectorStat<*>, *>> = materializeAs(concurrency, VectorStatSpec<*>::compileVector)

/** Materialize discrete-modality entries only; throws if any entry isn't discrete. */
fun StatSchemaDef.materializeDiscrete(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out DiscreteStat<*>, *>> = materializeAs(concurrency, DiscreteStatSpec<*>::compileDiscrete)

/** Materialize regression-modality entries only; throws if any entry isn't regression. */
fun StatSchemaDef.materializeRegression(
    concurrency: Concurrency = Concurrency.None,
): List<BoundStat<*, out RegressionStat<*>, *>> = materializeAs(concurrency, RegressionStatSpec<*>::compileRegression)

// Every entry compiles before any accumulator is built, so a bad entry fails the schema without side effects.
private inline fun <reified C : StatSpec, S : Stat<*>> StatSchemaDef.materializeAs(
    concurrency: Concurrency,
    compile: C.(String) -> StatPlan<S>,
): List<BoundStat<*, out S, *>> = stats.map { (name, config) ->
    require(config is C) {
        "Entry '$name' has config ${config::class.simpleName}, expected a ${C::class.simpleName}"
    }
    name to config.compile(name)
}.map { (name, plan) -> bind(name, plan.build(concurrency)) }

private fun <S : Stat<*>> bind(name: String, stat: S): BoundStat<*, out S, *> = toSpec(StatKey<Result>(name), stat)

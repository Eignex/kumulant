@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema.runtime

import com.eignex.kumulant.core.*
import com.eignex.kumulant.schema.spec.*

/**
 * Construct a live [SeriesStat] from a [SeriesStatSpec]. The specification tree is checked for
 * modality and result capabilities before constructing accumulators; invalid configurations name
 * the path through the specification tree.
 */
fun <R : Result> SeriesStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): SeriesStat<R> =
    compileSeries(this::class.simpleName ?: "SeriesStatSpec").build(concurrency) as SeriesStat<R>

/**
 * Construct a live [PairedStat] from a [PairedStatSpec]. The specification tree is checked for
 * modality and result capabilities before constructing accumulators; invalid configurations name
 * the path through the specification tree.
 */
fun <R : Result> PairedStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): PairedStat<R> =
    compilePaired(this::class.simpleName ?: "PairedStatSpec").build(concurrency) as PairedStat<R>

/**
 * Construct a live [VectorStat] from a [VectorStatSpec]. The specification tree is checked for
 * modality and result capabilities before constructing accumulators; invalid configurations name
 * the path through the specification tree.
 */
fun <R : Result> VectorStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): VectorStat<R> =
    compileVector(this::class.simpleName ?: "VectorStatSpec").build(concurrency) as VectorStat<R>

/**
 * Construct a live [DiscreteStat] from a [DiscreteStatSpec]. The specification tree is checked for
 * modality and result capabilities before constructing accumulators; invalid configurations name
 * the path through the specification tree.
 */
fun <R : Result> DiscreteStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): DiscreteStat<R> =
    compileDiscrete(this::class.simpleName ?: "DiscreteStatSpec").build(concurrency) as DiscreteStat<R>

/**
 * Construct a live [RegressionStat] from a [RegressionStatSpec]. The specification tree is checked for
 * modality and result capabilities before constructing accumulators; invalid configurations name
 * the path through the specification tree.
 */
fun <R : Result> RegressionStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): RegressionStat<R> =
    compileRegression(this::class.simpleName ?: "RegressionStatSpec").build(concurrency) as RegressionStat<R>

/** Construct a live stat from any [StatSpec], dispatching on its modality. */
fun StatSpec.materialize(concurrency: Concurrency = Concurrency.None): Stat<*> =
    compile(this::class.simpleName ?: "StatSpec").build(concurrency)

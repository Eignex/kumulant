package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.quantile.DDSketch
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.Sum
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Phase 1 [StatConfig] variants. Co-located here (rather than next to each
 * stat) because Kotlin requires direct subclasses of a sealed interface to
 * live in the same package as the interface.
 *
 * Each variant uses `@SerialName` matching its Kotlin class name, so the wire
 * `@type` value mirrors what a Kotlin reader would type. Defaults match the
 * underlying stat's primary constructor so authored payloads stay terse under
 * `encodeDefaults = false`.
 */

/** Configuration for [Mean]. No parameters. */
@Serializable
@SerialName("MeanConfig")
data object MeanConfig : SeriesStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedMeanResult> = Mean(concurrency)
}

/** Configuration for [Sum]. No parameters. */
@Serializable
@SerialName("SumConfig")
data object SumConfig : SeriesStatConfig<SumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SumResult> = Sum(concurrency)
}

/**
 * Configuration for [DDSketch]. Defaults match the live constructor.
 *
 * `probabilities` is a [List] on the wire because YAML renders lists more
 * cleanly than primitive arrays; it's converted to a `DoubleArray` at
 * [materialize] time.
 */
@Serializable
@SerialName("DDSketchConfig")
data class DDSketchConfig(
    val relativeError: Double = 0.01,
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatConfig<SketchResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SketchResult> =
        DDSketch(relativeError, probabilities.toDoubleArray(), concurrency)
}

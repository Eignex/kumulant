package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.schema.BandResult
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.stream.DEFAULT_TARGET_HIGH
import com.eignex.kumulant.stream.DEFAULT_TARGET_LOW
import com.eignex.kumulant.stream.DEFAULT_WINDOW_SLICES
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// Inner specs use the base type for polymorphic decoding; compilation validates their modality.
// Expression nodes serialize projections and compile to internal update closures.

@Serializable
@SerialName("WithWeightSeries")
internal data class WithWeightSeries(val inner: StatSpec, val weight: Double) : SeriesStatSpec<Result>

@Serializable
@SerialName("WithWeightPaired")
internal data class WithWeightPaired(val inner: StatSpec, val weight: Double) : PairedStatSpec<Result>

@Serializable
@SerialName("WithWeightVector")
internal data class WithWeightVector(val inner: StatSpec, val weight: Double) : VectorStatSpec<Result>

@Serializable
@SerialName("WithWeightDiscrete")
internal data class WithWeightDiscrete(val inner: StatSpec, val weight: Double) : DiscreteStatSpec<Result>

@Serializable
@SerialName("WithValueDiscrete")
internal data class WithValueDiscrete(val inner: StatSpec, val value: Long) : DiscreteStatSpec<Result>

@Serializable
@SerialName("AsSeries")
internal data class AsSeries(val inner: StatSpec) : SeriesStatSpec<Result>

@Serializable
@SerialName("AsDiscrete")
internal data class AsDiscrete(val inner: StatSpec) : DiscreteStatSpec<Result>

@Serializable
@SerialName("WithFixedX")
internal data class WithFixedX(val inner: StatSpec, val fixedX: Double) : SeriesStatSpec<Result>

@Serializable
@SerialName("WithFixedY")
internal data class WithFixedY(val inner: StatSpec, val fixedY: Double) : SeriesStatSpec<Result>

@Serializable
@SerialName("WithTimeAsX")
internal data class WithTimeAsX(val inner: StatSpec) : SeriesStatSpec<Result>

@Serializable
@SerialName("WithTimeAsY")
internal data class WithTimeAsY(val inner: StatSpec) : SeriesStatSpec<Result>

@Serializable
@SerialName("WindowedSeries")
internal data class WindowedSeries(
    val inner: StatSpec,
    val durationMillis: Long,
    val slices: Int = DEFAULT_WINDOW_SLICES,
) : SeriesStatSpec<Result>

@Serializable
@SerialName("WindowedPaired")
internal data class WindowedPaired(
    val inner: StatSpec,
    val durationMillis: Long,
    val slices: Int = DEFAULT_WINDOW_SLICES,
) : PairedStatSpec<Result>

@Serializable
@SerialName("WindowedVector")
internal data class WindowedVector(
    val inner: StatSpec,
    val durationMillis: Long,
    val slices: Int = DEFAULT_WINDOW_SLICES,
) : VectorStatSpec<Result>

@Serializable
@SerialName("WindowedDiscrete")
internal data class WindowedDiscrete(
    val inner: StatSpec,
    val durationMillis: Long,
    val slices: Int = DEFAULT_WINDOW_SLICES,
) : DiscreteStatSpec<Result>

@Serializable
@SerialName("Vectorized")
internal data class Vectorized(val dimensions: Int, val template: StatSpec, val skipZeros: Boolean = false) :
    VectorStatSpec<ResultList<Result>>

@Serializable
@SerialName("TransformValueSeries")
internal data class TransformValueSeries(val inner: StatSpec, val expr: ScalarExpr) : SeriesStatSpec<Result>

@Serializable
@SerialName("TransformValueDiscrete")
internal data class TransformValueDiscrete(val inner: StatSpec, val expr: ScalarExpr) : DiscreteStatSpec<Result>

@Serializable
@SerialName("FilterValueSeries")
internal data class FilterValueSeries(val inner: StatSpec, val pred: BoolExpr) : SeriesStatSpec<Result>

@Serializable
@SerialName("FilterValueDiscrete")
internal data class FilterValueDiscrete(val inner: StatSpec, val pred: BoolExpr) : DiscreteStatSpec<Result>

@Serializable
@SerialName("TransformPair")
internal data class TransformPair(val inner: StatSpec, val xExpr: ScalarExpr, val yExpr: ScalarExpr) :
    PairedStatSpec<Result>

@Serializable
@SerialName("FilterPaired")
internal data class FilterPaired(val inner: StatSpec, val pred: BoolExpr) : PairedStatSpec<Result>

@Serializable
@SerialName("TransformVectorElement")
internal data class TransformVectorElement(val inner: StatSpec, val expr: ScalarExpr) : VectorStatSpec<Result>

@Serializable
@SerialName("FilterVector")
internal data class FilterVector(val inner: StatSpec, val pred: BoolExpr) : VectorStatSpec<Result>

@Serializable
@SerialName("FoldPaired")
internal data class FoldPaired(val inner: StatSpec, val expr: ScalarExpr) : PairedStatSpec<Result>

@Serializable
@SerialName("FoldVector")
internal data class FoldVector(val inner: StatSpec, val expr: ScalarExpr) : VectorStatSpec<Result>

@Serializable
@SerialName("FoldVectorPaired")
internal data class FoldVectorPaired(val inner: StatSpec, val xExpr: ScalarExpr, val yExpr: ScalarExpr) :
    VectorStatSpec<Result>

@Serializable
@SerialName("TransformVector")
internal data class TransformVector(val inner: StatSpec, val expr: VectorExpr) : VectorStatSpec<Result>

@Serializable
@SerialName("WeightByValueSeries")
internal data class WeightByValueSeries(val inner: StatSpec, val expr: ScalarExpr) : SeriesStatSpec<Result>

@Serializable
@SerialName("WeightByValuePaired")
internal data class WeightByValuePaired(val inner: StatSpec, val expr: ScalarExpr) : PairedStatSpec<Result>

@Serializable
@SerialName("WeightByValueVector")
internal data class WeightByValueVector(val inner: StatSpec, val expr: ScalarExpr) : VectorStatSpec<Result>

@Serializable
@SerialName("WeightByValueDiscrete")
internal data class WeightByValueDiscrete(val inner: StatSpec, val expr: ScalarExpr) : DiscreteStatSpec<Result>

@Serializable
@SerialName("ThrottleSeries")
internal data class ThrottleSeries(val inner: StatSpec, val every: Int) : SeriesStatSpec<Result>

@Serializable
@SerialName("ThrottlePaired")
internal data class ThrottlePaired(val inner: StatSpec, val every: Int) : PairedStatSpec<Result>

@Serializable
@SerialName("ThrottleVector")
internal data class ThrottleVector(val inner: StatSpec, val every: Int) : VectorStatSpec<Result>

@Serializable
@SerialName("ThrottleDiscrete")
internal data class ThrottleDiscrete(val inner: StatSpec, val every: Int) : DiscreteStatSpec<Result>

@Serializable
@SerialName("DerivativeSeries")
internal data class DerivativeSeries(val inner: StatSpec) : SeriesStatSpec<Result>

@Serializable
@SerialName("HysteresisSeries")
internal data class HysteresisSeries(val inner: StatSpec, val low: Double, val high: Double) : SeriesStatSpec<Result>

@Serializable
@SerialName("ResampleByTimeSeries")
internal data class ResampleByTimeSeries(
    val inner: StatSpec,
    val bucketMillis: Long,
    val aggregator: ResampleAggregator = ResampleAggregator.Mean,
) : SeriesStatSpec<Result>

@Serializable
@SerialName("BandSeries")
internal data class BandSeries(val inner: StatSpec, val k: Double) : SeriesStatSpec<BandResult>

@Serializable
@SerialName("WithSelfLagSeries")
internal data class WithSelfLagSeries(val inner: StatSpec, val k: Int) : SeriesStatSpec<Result>

@Serializable
@SerialName("WithFeedbackSeries")
internal data class WithFeedbackSeries(val inner: StatSpec, val primary: StatSpec, val project: ScalarExpr) :
    SeriesStatSpec<Result>

@Serializable
@SerialName("WithFeedbackPaired")
internal data class WithFeedbackPaired(
    val inner: StatSpec,
    val primaryX: StatSpec,
    val primaryY: StatSpec,
    val project: ScalarExpr,
) : PairedStatSpec<Result>

@Serializable
@SerialName("WithFeedbackVector")
internal data class WithFeedbackVector(val inner: StatSpec, val primary: StatSpec, val project: ScalarExpr) :
    VectorStatSpec<Result>

@Serializable
@SerialName("WithFeedbackRegression")
internal data class WithFeedbackRegression(val inner: StatSpec, val primary: StatSpec, val project: ScalarExpr) :
    RegressionStatSpec<Result>

@Serializable
@SerialName("StandardScalerRegression")
internal data class StandardScalerRegression(val inner: StatSpec) : RegressionStatSpec<Result>

@Serializable
@SerialName("MinMaxScalerRegression")
internal data class MinMaxScalerRegression(
    val inner: StatSpec,
    val targetLow: Double = DEFAULT_TARGET_LOW,
    val targetHigh: Double = DEFAULT_TARGET_HIGH,
) : RegressionStatSpec<Result>

@Serializable
@SerialName("SampleSeries")
internal data class SampleSeries(val inner: StatSpec, val rate: Double, val seed: Long) : SeriesStatSpec<Result>

@Serializable
@SerialName("SamplePaired")
internal data class SamplePaired(val inner: StatSpec, val rate: Double, val seed: Long) : PairedStatSpec<Result>

@Serializable
@SerialName("SampleVector")
internal data class SampleVector(val inner: StatSpec, val rate: Double, val seed: Long) : VectorStatSpec<Result>

@Serializable
@SerialName("SampleDiscrete")
internal data class SampleDiscrete(val inner: StatSpec, val rate: Double, val seed: Long) : DiscreteStatSpec<Result>

@Serializable
@SerialName("FilterRegression")
internal data class FilterRegression(val inner: StatSpec, val pred: BoolExpr) : RegressionStatSpec<Result>

@Serializable
@SerialName("TransformYRegression")
internal data class TransformYRegression(val inner: StatSpec, val expr: ScalarExpr) : RegressionStatSpec<Result>

@Serializable
@SerialName("TransformXRegression")
internal data class TransformXRegression(val inner: StatSpec, val expr: VectorExpr, val featureSize: Int? = null) :
    RegressionStatSpec<Result>

@Serializable
@SerialName("WithWeightRegression")
internal data class WithWeightRegression(val inner: StatSpec, val weight: Double) : RegressionStatSpec<Result>

@Serializable
@SerialName("WeightByValueRegression")
internal data class WeightByValueRegression(val inner: StatSpec, val expr: ScalarExpr) : RegressionStatSpec<Result>

@Serializable
@SerialName("ThrottleRegression")
internal data class ThrottleRegression(val inner: StatSpec, val every: Int) : RegressionStatSpec<Result>

@Serializable
@SerialName("SampleRegression")
internal data class SampleRegression(val inner: StatSpec, val rate: Double, val seed: Long) :
    RegressionStatSpec<Result>

@Serializable
@SerialName("FoldRegression")
internal data class FoldRegression(val inner: StatSpec, val featureSize: Int, val project: ScalarExpr) :
    RegressionStatSpec<Result>

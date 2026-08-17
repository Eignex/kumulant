@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema.ops

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.schema.*
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.schema.spec.ResampleAggregator

// Composition operators for the wire specs in [com.eignex.kumulant.schema].
// Each returns a spec of the same modality, building the internal wrapper
// spec it corresponds to; AST-backed ops carry a ScalarExpr / BoolExpr.

/** Wrap this series spec so every update applies the per-observation [weight] multiplier. */
fun <R : Result> SeriesStatSpec<R>.withWeight(weight: Double): SeriesStatSpec<R> =
    WithWeightSeries(this, weight) as SeriesStatSpec<R>

/** Wrap this paired spec so every update applies the per-observation [weight] multiplier. */
fun <R : Result> PairedStatSpec<R>.withWeight(weight: Double): PairedStatSpec<R> =
    WithWeightPaired(this, weight) as PairedStatSpec<R>

/** Wrap this vector spec so every update applies the per-observation [weight] multiplier. */
fun <R : Result> VectorStatSpec<R>.withWeight(weight: Double): VectorStatSpec<R> =
    WithWeightVector(this, weight) as VectorStatSpec<R>

/** Wrap this discrete spec so every update applies the per-observation [weight] multiplier. */
fun <R : Result> DiscreteStatSpec<R>.withWeight(weight: Double): DiscreteStatSpec<R> =
    WithWeightDiscrete(this, weight) as DiscreteStatSpec<R>

/** Wrap this series spec so every update pushes the constant [value] regardless of input. */
fun <R : Result> SeriesStatSpec<R>.withValue(value: Double): SeriesStatSpec<R> =
    WithValueSeries(this, value) as SeriesStatSpec<R>

/** Wrap this discrete spec so every update pushes the constant [value] regardless of input. */
fun <R : Result> DiscreteStatSpec<R>.withValue(value: Long): DiscreteStatSpec<R> =
    WithValueDiscrete(this, value) as DiscreteStatSpec<R>

/** Adapt a discrete spec into a series spec - the series sees `value.toDouble()` per update. */
fun <R : Result> DiscreteStatSpec<R>.asSeries(): SeriesStatSpec<R> = AsSeries(this) as SeriesStatSpec<R>

/** Adapt a series spec into a discrete spec - the discrete sees `value.toLong()` per update. */
fun <R : Result> SeriesStatSpec<R>.asDiscrete(): DiscreteStatSpec<R> = AsDiscrete(this) as DiscreteStatSpec<R>

/** Adapt a series spec into a paired spec by consuming the `x` component of each pair. */
fun <R : Result> SeriesStatSpec<R>.atX(): PairedStatSpec<R> = AtX(this) as PairedStatSpec<R>

/** Adapt a series spec into a paired spec by consuming the `y` component of each pair. */
fun <R : Result> SeriesStatSpec<R>.atY(): PairedStatSpec<R> = AtY(this) as PairedStatSpec<R>

/** Adapt a series spec into a vector spec by consuming the [index]-th coordinate of each vector. */
fun <R : Result> SeriesStatSpec<R>.atIndex(index: Int): VectorStatSpec<R> = AtIndex(this, index) as VectorStatSpec<R>

/** Adapt a paired spec into a vector spec by consuming the [indexX] / [indexY] coordinates. */
fun <R : Result> PairedStatSpec<R>.atIndices(indexX: Int, indexY: Int): VectorStatSpec<R> =
    AtIndices(this, indexX, indexY) as VectorStatSpec<R>

/** Adapt a paired spec into a series spec by pinning `x` to [fixedX]. */
fun <R : Result> PairedStatSpec<R>.withFixedX(fixedX: Double): SeriesStatSpec<R> =
    WithFixedX(this, fixedX) as SeriesStatSpec<R>

/** Adapt a paired spec into a series spec by pinning `y` to [fixedY]. */
fun <R : Result> PairedStatSpec<R>.withFixedY(fixedY: Double): SeriesStatSpec<R> =
    WithFixedY(this, fixedY) as SeriesStatSpec<R>

/** Adapt a paired spec into a series spec by using the update timestamp as `x`. */
fun <R : Result> PairedStatSpec<R>.withTimeAsX(): SeriesStatSpec<R> = WithTimeAsX(this) as SeriesStatSpec<R>

/** Adapt a paired spec into a series spec by using the update timestamp as `y`. */
fun <R : Result> PairedStatSpec<R>.withTimeAsY(): SeriesStatSpec<R> = WithTimeAsY(this) as SeriesStatSpec<R>

/** Wrap this series spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> SeriesStatSpec<R>.windowed(durationMillis: Long, slices: Int = 10): SeriesStatSpec<R> =
    WindowedSeries(this, durationMillis, slices) as SeriesStatSpec<R>

/** Wrap this paired spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> PairedStatSpec<R>.windowed(durationMillis: Long, slices: Int = 10): PairedStatSpec<R> =
    WindowedPaired(this, durationMillis, slices) as PairedStatSpec<R>

/** Wrap this vector spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> VectorStatSpec<R>.windowed(durationMillis: Long, slices: Int = 10): VectorStatSpec<R> =
    WindowedVector(this, durationMillis, slices) as VectorStatSpec<R>

/** Wrap this discrete spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> DiscreteStatSpec<R>.windowed(durationMillis: Long, slices: Int = 10): DiscreteStatSpec<R> =
    WindowedDiscrete(this, durationMillis, slices) as DiscreteStatSpec<R>

/** Lift a series spec to a vector spec by replicating it across every coordinate of an N-dim input. */
fun <R : Result> SeriesStatSpec<R>.vectorized(
    dimensions: Int,
    skipZeros: Boolean = false,
): VectorStatSpec<ResultList<R>> = Vectorized(dimensions, this, skipZeros) as VectorStatSpec<ResultList<R>>

/** Wrap this series spec to apply [expr] to every update before the inner stat sees it. */
fun <R : Result> SeriesStatSpec<R>.transform(expr: ScalarExpr): SeriesStatSpec<R> =
    TransformValueSeries(this, expr) as SeriesStatSpec<R>

/** Wrap this discrete spec to apply [expr] to every update before the inner stat sees it. */
fun <R : Result> DiscreteStatSpec<R>.transform(expr: ScalarExpr): DiscreteStatSpec<R> =
    TransformValueDiscrete(this, expr) as DiscreteStatSpec<R>

/** Wrap this series spec so updates are forwarded only when [pred] evaluates true. */
fun <R : Result> SeriesStatSpec<R>.filter(pred: BoolExpr): SeriesStatSpec<R> =
    FilterValueSeries(this, pred) as SeriesStatSpec<R>

/** Wrap this discrete spec so updates are forwarded only when [pred] evaluates true. */
fun <R : Result> DiscreteStatSpec<R>.filter(pred: BoolExpr): DiscreteStatSpec<R> =
    FilterValueDiscrete(this, pred) as DiscreteStatSpec<R>

/** Wrap this paired spec so each `(x, y)` is remapped via [xExpr]/[yExpr] before the inner stat sees it. */
fun <R : Result> PairedStatSpec<R>.transformPair(xExpr: ScalarExpr, yExpr: ScalarExpr): PairedStatSpec<R> =
    TransformPair(this, xExpr, yExpr) as PairedStatSpec<R>

/** Map only the x coordinate; y stays as-is. */
fun <R : Result> PairedStatSpec<R>.transformX(expr: ScalarExpr): PairedStatSpec<R> = transformPair(expr, Y)

/** Map only the y coordinate; x stays as-is. */
fun <R : Result> PairedStatSpec<R>.transformY(expr: ScalarExpr): PairedStatSpec<R> = transformPair(X, expr)

/** Wrap this paired spec so updates are forwarded only when [pred] evaluates true on `(x, y)`. */
fun <R : Result> PairedStatSpec<R>.filter(pred: BoolExpr): PairedStatSpec<R> =
    FilterPaired(this, pred) as PairedStatSpec<R>

/** Wrap this vector spec to apply [expr] to every element of each incoming vector before update. */
fun <R : Result> VectorStatSpec<R>.transformElement(expr: ScalarExpr): VectorStatSpec<R> =
    TransformVectorElement(this, expr) as VectorStatSpec<R>

/** Wrap this vector spec so updates are forwarded only when [pred] evaluates true on the full vector. */
fun <R : Result> VectorStatSpec<R>.filter(pred: BoolExpr): VectorStatSpec<R> =
    FilterVector(this, pred) as VectorStatSpec<R>

/** Lift this series spec to a paired spec, reducing every `(x, y)` to a scalar via [expr]. */
fun <R : Result> SeriesStatSpec<R>.foldPaired(expr: ScalarExpr): PairedStatSpec<R> =
    FoldPaired(this, expr) as PairedStatSpec<R>

/** Lift this series spec to a vector spec, reducing every vector to a scalar via [expr]. */
fun <R : Result> SeriesStatSpec<R>.foldVector(expr: ScalarExpr): VectorStatSpec<R> =
    FoldVector(this, expr) as VectorStatSpec<R>

/**
 * Lift this paired spec to a vector spec, reducing every vector to a pair
 * `(xExpr, yExpr)` of scalars via [xExpr] and [yExpr].
 */
fun <R : Result> PairedStatSpec<R>.foldVector(xExpr: ScalarExpr, yExpr: ScalarExpr): VectorStatSpec<R> =
    FoldVectorPaired(this, xExpr, yExpr) as VectorStatSpec<R>

/** Wrap this vector spec so each incoming vector is remapped through [expr] before update. */
fun <R : Result> VectorStatSpec<R>.transformVector(expr: VectorExpr): VectorStatSpec<R> =
    TransformVector(this, expr) as VectorStatSpec<R>

/** Wrap this series spec so every update's weight is multiplied by `expr.eval(value)`. */
fun <R : Result> SeriesStatSpec<R>.weightBy(expr: ScalarExpr): SeriesStatSpec<R> =
    WeightByValueSeries(this, expr) as SeriesStatSpec<R>

/** Wrap this paired spec so every update's weight is multiplied by `expr.eval(x, y)`. */
fun <R : Result> PairedStatSpec<R>.weightBy(expr: ScalarExpr): PairedStatSpec<R> =
    WeightByValuePaired(this, expr) as PairedStatSpec<R>

/** Wrap this vector spec so every update's weight is multiplied by `expr.eval(0, 0, vec)`. */
fun <R : Result> VectorStatSpec<R>.weightBy(expr: ScalarExpr): VectorStatSpec<R> =
    WeightByValueVector(this, expr) as VectorStatSpec<R>

/** Wrap this discrete spec so every update's weight is multiplied by `expr.eval(value.toDouble())`. */
fun <R : Result> DiscreteStatSpec<R>.weightBy(expr: ScalarExpr): DiscreteStatSpec<R> =
    WeightByValueDiscrete(this, expr) as DiscreteStatSpec<R>

/** Wrap this series spec so it only sees one in every [every] updates. */
fun <R : Result> SeriesStatSpec<R>.throttle(every: Int): SeriesStatSpec<R> =
    ThrottleSeries(this, every) as SeriesStatSpec<R>

/** Wrap this paired spec so it only sees one in every [every] updates. */
fun <R : Result> PairedStatSpec<R>.throttle(every: Int): PairedStatSpec<R> =
    ThrottlePaired(this, every) as PairedStatSpec<R>

/** Wrap this vector spec so it only sees one in every [every] updates. */
fun <R : Result> VectorStatSpec<R>.throttle(every: Int): VectorStatSpec<R> =
    ThrottleVector(this, every) as VectorStatSpec<R>

/** Wrap this discrete spec so it only sees one in every [every] updates. */
fun <R : Result> DiscreteStatSpec<R>.throttle(every: Int): DiscreteStatSpec<R> =
    ThrottleDiscrete(this, every) as DiscreteStatSpec<R>

/** Wrap this series spec to forward the value seen [k] updates ago. */
fun <R : Result> SeriesStatSpec<R>.lag(k: Int): SeriesStatSpec<R> = LagSeries(this, k) as SeriesStatSpec<R>

/** Wrap this series spec to forward the k-th difference `value - value[t - k]`. */
fun <R : Result> SeriesStatSpec<R>.diff(k: Int = 1): SeriesStatSpec<R> = DiffSeries(this, k) as SeriesStatSpec<R>

/** Wrap this series spec to forward the per-second time derivative of the value stream. */
fun <R : Result> SeriesStatSpec<R>.derivative(): SeriesStatSpec<R> = DerivativeSeries(this) as SeriesStatSpec<R>

/** Wrap this series spec to debounce its input into a 0.0/1.0 stream via two-threshold hysteresis. */
fun <R : Result> SeriesStatSpec<R>.hysteresis(low: Double, high: Double): SeriesStatSpec<R> =
    HysteresisSeries(this, low, high) as SeriesStatSpec<R>

/** Wrap this series spec to forward one per-bucket summary using [aggregator]. */
fun <R : Result> SeriesStatSpec<R>.resampleByTime(
    bucketMillis: Long,
    aggregator: ResampleAggregator = ResampleAggregator.Mean,
): SeriesStatSpec<R> = ResampleByTimeSeries(this, bucketMillis, aggregator) as SeriesStatSpec<R>

/** Wrap this series spec to expose a `[lower, upper]` band of width [k] * scale around center. */
fun SeriesStatSpec<*>.band(k: Double): SeriesStatSpec<BandResult> = BandSeries(this, k)

/** Lift a paired spec into a series spec by self-pairing each input with the value seen [k] updates ago. */
fun <R : Result> PairedStatSpec<R>.withSelfLag(k: Int): SeriesStatSpec<R> =
    WithSelfLagSeries(this, k) as SeriesStatSpec<R>

/** Wrap this inner series spec with a feedback primary; the projection AST sees the primary snapshot. */
fun <R : Result> SeriesStatSpec<R>.withFeedback(primary: SeriesStatSpec<*>, project: ScalarExpr): SeriesStatSpec<R> =
    WithFeedbackSeries(this, primary, project) as SeriesStatSpec<R>

/** Element-wise standardise a vector spec against a hidden per-coordinate [Variance] primary. */
fun <R : Result> VectorStatSpec<R>.standardScaleFeatures(dimensions: Int): VectorStatSpec<R> =
    StandardScalerVector(this, dimensions) as VectorStatSpec<R>

/** Element-wise min-max scale a vector spec against a hidden per-coordinate [Range] primary. */
fun <R : Result> VectorStatSpec<R>.minMaxScaleFeatures(
    dimensions: Int,
    targetLow: Double = 0.0,
    targetHigh: Double = 1.0,
): VectorStatSpec<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    return MinMaxScalerVector(this, dimensions, targetLow, targetHigh) as VectorStatSpec<R>
}

/** Z-score both axes of a paired spec against per-axis [Variance] primaries. */
fun <R : Result> PairedStatSpec<R>.standardScaler(): PairedStatSpec<R> = StandardScalerPaired(this) as PairedStatSpec<R>

/** Min-max scale both axes of a paired spec against per-axis [Range] primaries. */
fun <R : Result> PairedStatSpec<R>.minMaxScaler(targetLow: Double = 0.0, targetHigh: Double = 1.0): PairedStatSpec<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    return MinMaxScalerPaired(this, targetLow, targetHigh) as PairedStatSpec<R>
}

/** Element-wise standardise a regression spec's feature vector. */
fun <R : Result> RegressionStatSpec<R>.standardScaleFeatures(): RegressionStatSpec<R> =
    StandardScalerRegression(this) as RegressionStatSpec<R>

/** Element-wise min-max scale a regression spec's feature vector. */
fun <R : Result> RegressionStatSpec<R>.minMaxScaleFeatures(
    targetLow: Double = 0.0,
    targetHigh: Double = 1.0,
): RegressionStatSpec<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    return MinMaxScalerRegression(this, targetLow, targetHigh) as RegressionStatSpec<R>
}

/**
 * Z-score the input against a hidden [Variance] primary, then forward the standardized
 * value to this spec. Emits `0` while the running variance is still zero.
 */
fun <R : Result> SeriesStatSpec<R>.standardScaler(): SeriesStatSpec<R> = StandardScalerSeries(this) as SeriesStatSpec<R>

/**
 * Min-max scale the input against a hidden [Range] primary into `[targetLow, targetHigh]`,
 * then forward the mapped value to this spec. Defaults map to `[0, 1]`; pass
 * `targetLow = -1.0, targetHigh = 1.0` for a `[-1, 1]` mapping. Emits [targetLow] while
 * the running range is still degenerate.
 */
fun <R : Result> SeriesStatSpec<R>.minMaxScaler(targetLow: Double = 0.0, targetHigh: Double = 1.0): SeriesStatSpec<R> {
    require(targetHigh > targetLow) { "targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    return MinMaxScalerSeries(this, targetLow, targetHigh) as SeriesStatSpec<R>
}

/** Wrap this series spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> SeriesStatSpec<R>.sample(rate: Double, seed: Long): SeriesStatSpec<R> =
    SampleSeries(this, rate, seed) as SeriesStatSpec<R>

/** Wrap this paired spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> PairedStatSpec<R>.sample(rate: Double, seed: Long): PairedStatSpec<R> =
    SamplePaired(this, rate, seed) as PairedStatSpec<R>

/** Wrap this vector spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> VectorStatSpec<R>.sample(rate: Double, seed: Long): VectorStatSpec<R> =
    SampleVector(this, rate, seed) as VectorStatSpec<R>

/** Wrap this discrete spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> DiscreteStatSpec<R>.sample(rate: Double, seed: Long): DiscreteStatSpec<R> =
    SampleDiscrete(this, rate, seed) as DiscreteStatSpec<R>

/** Wrap this regression spec so updates are forwarded only when [pred] evaluates true. */
fun <R : Result> RegressionStatSpec<R>.filter(pred: BoolExpr): RegressionStatSpec<R> =
    FilterRegression(this, pred) as RegressionStatSpec<R>

/**
 * Drop any update carrying a non-finite input, so the stat only ever sees real numbers.
 *
 * [Stat][com.eignex.kumulant.core.Stat] guarantees one thing about a non-finite value: it will not
 * throw. It does not guarantee the stat stays usable. An accumulator folds a `NaN` in and reports `NaN`
 * from then on, and no later observation clears it - which is correct, because the observation really did
 * arrive and really was unusable. This is the supported way to say the stream should not contain such
 * observations in the first place.
 *
 * Applies to whichever inputs the modality has: the value for a series stat, both coordinates for a
 * paired stat, every coordinate for a vector stat, and the target plus every feature for a regression
 * stat. There is no discrete counterpart because a `Long` cannot be non-finite.
 *
 * ```
 * Mean.filterFinite()
 * StochasticRegression(featureSize = 8).filterFinite()
 * ```
 */
fun <R : Result> SeriesStatSpec<R>.filterFinite(): SeriesStatSpec<R> = filter(allFinite())

/** Paired counterpart of [SeriesStatSpec.filterFinite]; both `x` and `y` must be finite. */
fun <R : Result> PairedStatSpec<R>.filterFinite(): PairedStatSpec<R> = filter(allFinite())

/** Vector counterpart of [SeriesStatSpec.filterFinite]; every coordinate must be finite. */
fun <R : Result> VectorStatSpec<R>.filterFinite(): VectorStatSpec<R> = filter(allFinite())

/** Regression counterpart of [SeriesStatSpec.filterFinite]; the target and every feature must be finite. */
fun <R : Result> RegressionStatSpec<R>.filterFinite(): RegressionStatSpec<R> = filter(allFinite())

/** Wrap this regression spec so y is remapped by [expr] before the inner stat sees it. */
fun <R : Result> RegressionStatSpec<R>.transformY(expr: ScalarExpr): RegressionStatSpec<R> =
    TransformYRegression(this, expr) as RegressionStatSpec<R>

/** Wrap this regression spec so x is remapped by [expr] before the inner stat sees it. */
fun <R : Result> RegressionStatSpec<R>.transformX(expr: VectorExpr): RegressionStatSpec<R> =
    TransformXRegression(this, expr) as RegressionStatSpec<R>

/** Wrap this regression spec so every update uses [weight] regardless of caller input. */
fun <R : Result> RegressionStatSpec<R>.withWeight(weight: Double): RegressionStatSpec<R> =
    WithWeightRegression(this, weight) as RegressionStatSpec<R>

/** Wrap this regression spec so every update's weight is multiplied by `expr.eval(0, y, v)`. */
fun <R : Result> RegressionStatSpec<R>.weightBy(expr: ScalarExpr): RegressionStatSpec<R> =
    WeightByValueRegression(this, expr) as RegressionStatSpec<R>

/** Wrap this regression spec so it only sees one in every [every] updates. */
fun <R : Result> RegressionStatSpec<R>.throttle(every: Int): RegressionStatSpec<R> =
    ThrottleRegression(this, every) as RegressionStatSpec<R>

/** Wrap this regression spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> RegressionStatSpec<R>.sample(rate: Double, seed: Long): RegressionStatSpec<R> =
    SampleRegression(this, rate, seed) as RegressionStatSpec<R>

/** Lift this series spec into the regression modality. [project] reduces each `(x = V, y = Y)`
 *  update to a scalar that the inner series stat absorbs. Use `Y` for the marginal-y view. */
fun <R : Result> SeriesStatSpec<R>.foldRegression(featureSize: Int, project: ScalarExpr): RegressionStatSpec<R> =
    FoldRegression(this, featureSize, project) as RegressionStatSpec<R>

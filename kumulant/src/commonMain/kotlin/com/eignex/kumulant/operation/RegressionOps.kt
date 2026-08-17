package com.eignex.kumulant.operation

import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import kotlin.random.Random

// Decorator surface for [RegressionStat], mirroring the live ops on the other modalities.
// Every wrapper delegates `read` / `merge` / `reset` / `concurrency` / `featureSize` to
// the inner regressor and intercepts only `update` to filter, transform, weight,
// throttle, sample, or fan out. Spec-layer counterparts in
// `com.eignex.kumulant.schema.Operations.kt` materialise to these wrappers.

/** Forward only updates that pass [predicate]; the predicate sees `(x, y)`. */
internal fun <R : Result> RegressionStat<R>.filter(predicate: (VectorView, Double) -> Boolean): RegressionStat<R> =
    FilterRegressionStat(this, predicate)

/** Rewrite y before update via [transform]; x and weight pass through unchanged. */
internal fun <R : Result> RegressionStat<R>.transformY(transform: (VectorView, Double) -> Double): RegressionStat<R> =
    TransformYRegressionStat(this, transform)

/** Rewrite x before update via [transform]; y and weight pass through unchanged. */
internal fun <R : Result> RegressionStat<R>.transformX(
    transform: (VectorView, Double) -> DoubleArray,
): RegressionStat<R> = TransformXRegressionStat(this, transform)

/**
 * Replace every update's weight with the constant [weight].
 *
 * An inert caller weight is passed through rather than replaced, exactly as in the other four
 * modalities; see [com.eignex.kumulant.operation.withWeight] for why.
 */
internal fun <R : Result> RegressionStat<R>.withWeight(weight: Double): RegressionStat<R> =
    WithWeightRegressionStat(this, weight)

/** Multiply each update's caller-supplied weight by [weighter] over `(x, y)`. */
internal fun <R : Result> RegressionStat<R>.weightBy(weighter: (VectorView, Double) -> Double): RegressionStat<R> =
    WeightByRegressionStat(this, weighter)

/** Forward only every [every]th update; drop the rest. */
internal fun <R : Result> RegressionStat<R>.throttle(every: Int): RegressionStat<R> = ThrottleRegressionStat(
    this,
    every,
)

/** Bernoulli-sample each update at probability [rate] using [random] as the PRNG. */
internal fun <R : Result> RegressionStat<R>.sample(rate: Double, random: Random): RegressionStat<R> =
    SampleRegressionStat(this, rate, random)

/**
 * Lift a scalar [SeriesStat] into a [RegressionStat] by projecting `(x, y)` to a
 * scalar via [project]. Use it to fan a regression input stream into a series stat
 * that only cares about, say, the marginal y distribution: pass `{ _, y -> y }`.
 *
 * The resulting RegressionStat's `read` returns the underlying SeriesStat's result;
 * `featureSize` is the caller-supplied [featureSize] so an x-vector contract is
 * enforced at update time even though the inner stat ignores x.
 */
internal fun <R : Result> SeriesStat<R>.foldRegression(
    featureSize: Int,
    project: (VectorView, Double) -> Double,
): RegressionStat<R> = FoldRegressionStat(this, featureSize, project)

internal class FilterRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val predicate: (VectorView, Double) -> Boolean,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        if (predicate(x, y)) delegate.update(x, y, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        FilterRegressionStat(delegate.create(concurrency), predicate)
}

internal class TransformYRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val transform: (VectorView, Double) -> Double,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, transform(x, y), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        TransformYRegressionStat(delegate.create(concurrency), transform)
}

internal class TransformXRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val transform: (VectorView, Double) -> DoubleArray,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(DenseVector.of(transform(x, y)), y, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        TransformXRegressionStat(delegate.create(concurrency), transform)
}

internal class WithWeightRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val weight: Double,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        // `orInert` covers NaN as well as zero, so an inert caller weight stays a no-op rather than
        // being replaced by the constant and becoming a real observation.
        delegate.update(x, y, timestampNanos, weight.orInert(this.weight))
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        WithWeightRegressionStat(delegate.create(concurrency), weight)
}

internal class WeightByRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val weighter: (VectorView, Double) -> Double,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, y, timestampNanos, weight * weighter(x, y))
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        WeightByRegressionStat(delegate.create(concurrency), weighter)
}

internal class ThrottleRegressionStat<R : Result>(private val delegate: RegressionStat<R>, private val every: Int) :
    RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    private val gate = ThrottleGate(every, delegate.concurrency)
    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(x, y, timestampNanos, weight)
    }
    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        ThrottleRegressionStat(delegate.create(concurrency), every)
}

internal class SampleRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val rate: Double,
    private val random: Random,
) : RegressionStat<R>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, random)
    override val featureSize: Int = delegate.featureSize
    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        if (gate.pass()) delegate.update(x, y, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        SampleRegressionStat(delegate.create(concurrency), rate, random)
}

internal class FoldRegressionStat<R : Result>(
    private val delegate: SeriesStat<R>,
    override val featureSize: Int,
    private val project: (VectorView, Double) -> Double,
) : RegressionStat<R>,
    Stat<R> by delegate {
    init {
        requirePositiveFeatureSize(featureSize)
    }
    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        delegate.update(project(x, y), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        FoldRegressionStat(delegate.create(concurrency), featureSize, project)
}

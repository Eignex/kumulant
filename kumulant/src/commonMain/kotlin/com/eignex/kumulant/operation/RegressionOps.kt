package com.eignex.kumulant.operation

import com.eignex.koblas.DenseVector
import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize

internal fun <R : Result> RegressionStat<R>.filter(predicate: (Vector, Double) -> Boolean): RegressionStat<R> =
    FilterRegressionStat(this, predicate)

internal fun <R : Result> RegressionStat<R>.transformY(transform: (Vector, Double) -> Double): RegressionStat<R> =
    TransformYRegressionStat(this, transform)

// inputFeatureSize describes the vector before transformation; null uses the inner width.
internal fun <R : Result> RegressionStat<R>.transformX(
    inputFeatureSize: Int? = null,
    transform: (Vector, Double) -> DoubleArray,
): RegressionStat<R> = TransformXRegressionStat(this, inputFeatureSize, transform)

// Inert caller weights pass through unchanged to preserve the no-op contract.
internal fun <R : Result> RegressionStat<R>.withWeight(weight: Double): RegressionStat<R> =
    WithWeightRegressionStat(this, weight)

internal fun <R : Result> RegressionStat<R>.weightBy(weighter: (Vector, Double) -> Double): RegressionStat<R> =
    WeightByRegressionStat(this, weighter)

internal fun <R : Result> RegressionStat<R>.throttle(every: Int): RegressionStat<R> = ThrottleRegressionStat(
    this,
    every,
)

internal fun <R : Result> RegressionStat<R>.sample(rate: Double, seed: Long): RegressionStat<R> =
    SampleRegressionStat(this, rate, seed)

internal fun <R : Result> SeriesStat<R>.foldRegression(
    featureSize: Int,
    project: (Vector, Double) -> Double,
): RegressionStat<R> = FoldRegressionStat(this, featureSize, project)

internal class FilterRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val predicate: (Vector, Double) -> Boolean,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        if (predicate(x, y)) delegate.update(x, y, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        FilterRegressionStat(delegate.create(concurrency), predicate)
}

internal class TransformYRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val transform: (Vector, Double) -> Double,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, transform(x, y), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        TransformYRegressionStat(delegate.create(concurrency), transform)
}

internal class TransformXRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val inputFeatureSize: Int?,
    private val transform: (Vector, Double) -> DoubleArray,
) : RegressionStat<R>,
    Stat<R> by delegate {
    // The width expected in `x`, which is what RegressionStat.featureSize means - not the inner
    // regressor's. Advertising the post-transform width would make a caller sizing its input from this
    // send the wrong number of coordinates.
    override val featureSize: Int = inputFeatureSize ?: delegate.featureSize

    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        delegate.update(DenseVector.of(transform(x, y)), y, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        TransformXRegressionStat(delegate.create(concurrency), inputFeatureSize, transform)
}

internal class WithWeightRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val weight: Double,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        // `orInert` covers NaN as well as zero, so an inert caller weight stays a no-op rather than
        // being replaced by the constant and becoming a real observation.
        delegate.update(x, y, timestampNanos, weight.orInert(this.weight))
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        WithWeightRegressionStat(delegate.create(concurrency), weight)
}

internal class WeightByRegressionStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val weighter: (Vector, Double) -> Double,
) : RegressionStat<R>,
    Stat<R> by delegate {
    override val featureSize: Int = delegate.featureSize
    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
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
    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
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
    private val seed: Long,
) : RegressionStat<R>,
    Stat<R> by delegate {
    private val gate = SampleGate(rate, seed, delegate.concurrency)
    override val featureSize: Int = delegate.featureSize
    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (gate.pass()) delegate.update(x, y, timestampNanos, weight)
    }

    override fun reset() {
        gate.reset()
        delegate.reset()
    }

    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        SampleRegressionStat(delegate.create(concurrency), rate, gate.childSeed())
}

internal class FoldRegressionStat<R : Result>(
    private val delegate: SeriesStat<R>,
    override val featureSize: Int,
    private val project: (Vector, Double) -> Double,
) : RegressionStat<R>,
    Stat<R> by delegate {
    init {
        requirePositiveFeatureSize(featureSize)
    }
    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        delegate.update(project(x, y), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        FoldRegressionStat(delegate.create(concurrency), featureSize, project)
}

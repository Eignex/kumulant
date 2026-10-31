package com.eignex.kumulant.core

import com.eignex.koblas.DenseVector
import com.eignex.koblas.Vector
import com.eignex.kumulant.stream.currentTimeNanos

/**
 * Base contract for statistical accumulators. Observations update mutable state;
 * [read] returns a [Result] snapshot and [merge] folds a compatible snapshot into it.
 *
 * Input shapes are defined by [SeriesStat], [DiscreteStat], [PairedStat],
 * [VectorStat], and [RegressionStat]. Their update methods share these weight rules:
 *
 * - `1.0` is the unweighted case; other finite weights scale the observation's contribution.
 * - Zero and non-finite weights are no-ops.
 * - Negative weights downdate statistics with an inverse. Other statistics reject or
 *   ignore them as documented by the implementation. Welford accumulators reject a
 *   downdate that would leave a non-positive total weight.
 *
 * Non-finite observation values follow each statistic's arithmetic and comparisons:
 * means can become `NaN`, extrema ignore `NaN`, and buckets may use a fallthrough bin.
 * Domain restrictions still apply, such as non-negative histogram inputs. Classifiers
 * ignore labels that are not exact integers in their configured class range.
 * Use [filterFinite][com.eignex.kumulant.schema.ops.filterFinite] to discard updates
 * with non-finite inputs before they reach the accumulator.
 *
 * Timestamps are nanoseconds from a consistent clock. Default timestamps use a
 * process-local monotonic clock; replayed updates and reads must use a common origin.
 * Time-independent statistics ignore timestamps.
 *
 * Every built-in stat merges compatible snapshots. Exact formulas are used where
 * sufficient state is available; implementations document approximations and any
 * state omitted from snapshots. Thread safety follows [concurrency].
 *
 * @param R Snapshot type returned by [read].
 * @sample com.eignex.kumulant.samples.basicMeanLifecycle
 */
interface Stat<R : Result> {
    /**
     * Thread-safety mode fixed at construction. See [Concurrency] for the shared
     * contract and the implementation's KDoc for its cell and lock strategy.
     */
    val concurrency: Concurrency

    /**
     * Validate [values] for [merge] without changing accumulator state.
     * Containers validate every child before merging any of them. Successful validation
     * checks compatibility; it does not make a concurrent merge transactional.
     */
    fun validateMerge(values: R) = Unit

    /**
     * Fold a compatible snapshot [values] into this accumulator at [timestampNanos].
     * Implementations document whether the combine is exact or approximate and which
     * state the snapshot omits. The source accumulator is unaffected.
     *
     * Time-dependent statistics, such as windows, place the snapshot at [timestampNanos], which
     * must share the clock the caller uses for updates and reads. Time-independent statistics
     * ignore it.
     */
    fun merge(values: R, timestampNanos: Long = currentTimeNanos())

    /**
     * Restore the configured baseline, including any prior seed.
     * Preserves the configuration and [concurrency].
     */
    fun reset()

    /**
     * Return the current result evaluated at [timestampNanos].
     * Time-independent statistics ignore the timestamp. Reading does not add an observation.
     *
     * Snapshot consistency depends on the implementation's synchronisation. Locks protect
     * coupled state under [Concurrency.Strict] and [Concurrency.HighWrite]; independently
     * updated cells and JVM striped adders can still yield a best-effort snapshot.
     * [Concurrency.Relaxed] permits drift across coupled cells.
     */
    fun read(timestampNanos: Long = currentTimeNanos()): R

    /**
     * Create an independent accumulator at the configured baseline.
     * A null [concurrency] preserves this accumulator's mode; a non-null value overrides it.
     * Modality subtypes narrow the return type to preserve the input shape.
     */
    fun create(concurrency: Concurrency? = null): Stat<R>
}

/**
 * Accumulator over scalar `Double` observations, including descriptive statistics,
 * quantile sketches, rates, and decay statistics. See [Stat] for weight and timestamp rules.
 */
interface SeriesStat<R : Result> : Stat<R> {
    /** Record [value] with [weight] at the current monotonic time. */
    fun update(value: Double, weight: Double = 1.0) = update(value, currentTimeNanos(), weight)

    /** Record [value] with [weight] at [timestampNanos]. */
    fun update(value: Double, timestampNanos: Long, weight: Double = 1.0)

    override fun create(concurrency: Concurrency?): SeriesStat<R>
}

/**
 * Accumulator over a stream of discrete `Long` values. The `Long` carries
 * two interpretations across the family:
 *
 * - **Opaque keys**: cardinality estimators
 *   ([HyperLogLogStat][com.eignex.kumulant.stat.cardinality.HyperLogLogStat]),
 *   heavy-hitter sketches
 *   ([SpaceSavingStat][com.eignex.kumulant.stat.sketch.SpaceSavingStat]),
 *   Bloom filters
 *   ([BloomFilterStat][com.eignex.kumulant.stat.sketch.BloomFilterStat]).
 *   The numeric value of the `Long` is irrelevant; only equality matters.
 *   Hash domain-specific keys through [com.eignex.kumulant.math.hash64] first
 *   so the input carries uniform 64-bit entropy.
 *
 * - **Integer-valued measurements**: Poisson counts, time deltas, integer
 *   histograms. Here the value is meaningful and arithmetic is applied to it.
 *
 * Each concrete stat documents which interpretation it uses.
 */
interface DiscreteStat<R : Result> : Stat<R> {
    /** Record [value] with [weight] at the current monotonic time. */
    fun update(value: Long, weight: Double = 1.0) = update(value, currentTimeNanos(), weight)

    /** Record [value] with [weight] at [timestampNanos]. */
    fun update(value: Long, timestampNanos: Long, weight: Double = 1.0)

    override fun create(concurrency: Concurrency?): DiscreteStat<R>
}

/**
 * Accumulator over paired `(x, y)` scalar observations. The shape covers
 * scalar-on-scalar regression
 * ([UnivariateRegressionStat][com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat]),
 * weighted covariance and correlation
 * ([CovarianceStat][com.eignex.kumulant.stat.regression.CovarianceStat]),
 * and every paired evaluation metric in [com.eignex.kumulant.stat.score] /
 * [com.eignex.kumulant.stat.calibration]:
 * `(prediction, truth)` pairs, `(score, label)` pairs, etc.
 *
 * Convention across the library: `x` is the predictor / input axis, `y` is
 * the response / outcome axis. Score-family stats treat `x` as the predicted
 * value and `y` as the observed value.
 */
interface PairedStat<R : Result> : Stat<R> {
    /** Record the pair ([x], [y]) with [weight] at the current monotonic time. */
    fun update(x: Double, y: Double, weight: Double = 1.0) = update(x, y, currentTimeNanos(), weight)

    /** Record the pair ([x], [y]) with [weight] at [timestampNanos]. */
    fun update(x: Double, y: Double, timestampNanos: Long, weight: Double = 1.0)

    override fun create(concurrency: Concurrency?): PairedStat<R>
}

/**
 * Accumulator over vector-covariate / scalar-response observations
 * `(x, y, weight)`, where `x` is a fixed-dimensional feature vector and `y`
 * is the scalar target. The multivariate generalisation of [PairedStat] and
 * the input shape for every linear / non-linear regressor.
 *
 * Implementations cover the full spread from a one-pass SGD weight tracker
 * ([StochasticRegressionStat][com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat])
 * to a full Bayesian linear regression with a joint posterior
 * ([BayesianRegressionStat][com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat])
 * to a non-linear decision tree
 * ([DecisionTreeRegressionStat][com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat]).
 * They share the update shape and differ in what they expose on [read].
 *
 * Inputs are passed as [Vector] so callers can submit sparse feature
 * vectors without materialising them into dense arrays first. The
 * [DoubleArray] convenience overloads wrap the array in a [DenseVector]; the
 * sparse path goes through [com.eignex.koblas.SparseVector].
 *
 * The K-way classifiers
 * ([SoftmaxRegressionStat][com.eignex.kumulant.stat.regression.SoftmaxRegressionStat],
 * [GaussianNaiveBayesStat][com.eignex.kumulant.stat.regression.GaussianNaiveBayesStat])
 * also use this interface; `y` is the class index in `[0, numClasses)`.
 */
interface RegressionStat<R : Result> : Stat<R> {
    /** Number of features expected in `x` on each [update]. Mismatched lengths throw. */
    val featureSize: Int

    /** Record the pair ([x], [y]) with [weight] at the current monotonic time. */
    fun update(x: Vector, y: Double, weight: Double = 1.0) = update(x, y, currentTimeNanos(), weight)

    /** Record the pair ([x], [y]) with [weight] at [timestampNanos]. */
    fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double = 1.0)

    /** Convenience overload that wraps `x` as a [DenseVector]. */
    fun update(x: DoubleArray, y: Double, weight: Double = 1.0) =
        update(DenseVector.of(x), y, currentTimeNanos(), weight)

    /** Timestamped convenience overload that wraps `x` as a [DenseVector]. */
    fun update(x: DoubleArray, y: Double, timestampNanos: Long, weight: Double = 1.0) =
        update(DenseVector.of(x), y, timestampNanos, weight)

    override fun create(concurrency: Concurrency?): RegressionStat<R>
}

/**
 * Accumulator over fixed-dimensional vector observations without a response
 * axis. The natural fit for per-coordinate aggregations
 * ([VectorizedStat][com.eignex.kumulant.schema.spec.Vectorized]'s fan-out of any
 * series stat across `dimensions` channels).
 *
 * Like [RegressionStat], inputs are passed as [Vector] so sparse callers
 * don't pay for dense materialisation. The [DoubleArray] convenience
 * overloads wrap the array in a [DenseVector].
 */
interface VectorStat<R : Result> : Stat<R> {
    /** Record [vector] with [weight] at the current monotonic time. */
    fun update(vector: Vector, weight: Double = 1.0) = update(vector, currentTimeNanos(), weight)

    /** Record [vector] with [weight] at [timestampNanos]. */
    fun update(vector: Vector, timestampNanos: Long, weight: Double = 1.0)

    /** Convenience overload that wraps [vector] as a [DenseVector]. */
    fun update(vector: DoubleArray, weight: Double = 1.0) = update(
        DenseVector.of(vector),
        currentTimeNanos(),
        weight,
    )

    /** Timestamped convenience overload that wraps [vector] as a [DenseVector]. */
    fun update(vector: DoubleArray, timestampNanos: Long, weight: Double = 1.0) =
        update(DenseVector.of(vector), timestampNanos, weight)

    override fun create(concurrency: Concurrency?): VectorStat<R>
}

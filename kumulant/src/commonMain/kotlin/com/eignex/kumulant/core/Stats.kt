package com.eignex.kumulant.core

import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorView
import com.eignex.kumulant.stream.currentTimeNanos

/**
 * The base interface for all statistical accumulators. Implementations
 * accumulate a streaming view of some input, expose the current state as an
 * immutable [Result] via [read], and merge another snapshot in via [merge].
 *
 * Five modality sub-interfaces refine the [Stat] contract by the shape of the
 * input observation: [SeriesStat] / [DiscreteStat] / [PairedStat] /
 * [VectorStat] / [RegressionStat]. Picking the right modality is the first
 * design decision when adding a new stat; everything else falls out of it.
 *
 * The full lifecycle (`update` / `read` / `merge`) is shown end-to-end below.
 *
 * Every modality's `update` takes a per-observation `weight`, interpreted
 * consistently with the stat's mathematical role: weighted means take it as the
 * observation weight, sums multiply by it, histograms add it to the destination
 * bin. A weight of 1 (the default) is the unweighted case.
 *
 * Two guarantees hold across every stat:
 *
 * - A weight of `0.0` is a no-op. The observation is not folded in and no state
 *   changes, whatever the modality.
 * - A negative weight carries whatever meaning is standard for that particular
 *   statistic. For the accumulating families that is a downdate: it removes an
 *   observation previously folded in. Where a statistic has no sensible inverse,
 *   or where the update would corrupt the accumulator rather than merely give a
 *   surprising answer, it throws [IllegalArgumentException] instead. Each stat's
 *   own KDoc says which applies; there is deliberately no library-wide answer,
 *   because subtraction is well defined for some statistics and not others.
 *
 * A third guarantee covers the observation rather than its weight:
 *
 * - A `NaN` observation is dropped, exactly like a zero weight: no state changes.
 *   `NaN` is absence of a measurement, not a measurement, and it has no position
 *   in any ordering, so every alternative is worse. Absorbing it silently moves
 *   extrema and counters (`NaN >= level` is false, so it would land below every
 *   threshold), and throwing turns a gap in the input into an outage in the
 *   caller. This applies to the value only; `NaN` in a *weight* is the caller
 *   asking for nonsense and is not guarded.
 *
 * One statistic is deliberately exempt from the `NaN` rule:
 * [RunLengthStat][com.eignex.kumulant.stat.event.RunLengthStat] takes a predicate
 * projected onto `0.0` / `1.0` rather than a measurement, so for it `NaN` reads as
 * "not satisfied" and breaks the run rather than being ignored.
 *
 * Checks beyond that are the caller's responsibility. Stats validate only where
 * the alternative is corrupted state, not to police inputs.
 *
 * How that resolves per family, surveyed across the catalogue rather than assumed:
 *
 * - **Additive** (sums, counts, rates, decayed sums): subtraction, exactly as you
 *   would expect. Nothing to guard.
 * - **Welford** (mean, variance, moments): a downdate that inverts the update
 *   exactly. The one rejected case is a downdate that would take the accumulated
 *   weight to zero or below, since every step divides by the new total and the
 *   accumulator would be left permanently non-finite.
 * - **EWMA and decay**: a downdate. Over-subtracting drives the accumulated weight
 *   negative, which is reported rather than hidden and recovers as soon as positive
 *   weight arrives. [DecayingMeanStat][com.eignex.kumulant.stat.decay.DecayingMeanStat]
 *   reports `NaN` while the decayed weight is negative, since there is no meaningful
 *   mean of a negative amount of evidence; that is a sentinel, not a wedged state.
 * - **Counting** (histograms, threshold buckets): the bin is a signed accumulation
 *   rather than a population count, so subtraction can take a bin below zero.
 *   Guarding it would break the legitimate case of retracting an earlier observation.
 * - **Monotone sketches** (HyperLogLog, Bloom, MinHash): no inverse exists, since
 *   these only ever set bits or take maxima. They ignore non-positive weights.
 *
 * `NegativeWeightSemanticsTest` pins each of these.
 *
 * @param R The result type returned by [read]; always a [Result] subtype.
 *
 * @sample com.eignex.kumulant.samples.basicMeanLifecycle
 */
interface Stat<R : Result> {
    /**
     * The thread-safety contract this stat was constructed with. Each stat
     * picks the cell-encoding and lock strategy that honours this contract
     * for its mathematical structure:
     *
     * - [Concurrency.None]: single-threaded; no synchronisation. Cheapest path.
     * - [Concurrency.Relaxed]: lock-free best-effort. Multi-cell stats
     *   (Welford-style [MeanStat][com.eignex.kumulant.stat.summary.MeanStat],
     *   [VarianceStat][com.eignex.kumulant.stat.summary.VarianceStat],
     *   [MomentsStat][com.eignex.kumulant.stat.summary.MomentsStat]) may drift
     *   under contention but never throw.
     * - [Concurrency.Strict]: serialised when needed for full correctness
     *   across coupled cells. Sketches always self-serialise; Welford stats
     *   lock per update.
     * - [Concurrency.HighWrite]: optimised for many concurrent writers; JVM
     *   uses striped adders for naively additive stats.
     *
     * Picked at construction; immutable after.
     */
    val concurrency: Concurrency

    /**
     * Fold another accumulator's snapshot into this one. The unit of merge is
     * the immutable [Result]; not a live [Stat]; which is what lets the merge
     * cross a process boundary. Many workers track slices of the same stream,
     * call [read] periodically, ship snapshots to a coordinator, and the
     * coordinator merges them in.
     *
     * Most stat families implement merge exactly (Chan-style parallel formulas
     * for Welford, cell-wise additions for histograms, cell-wise max for HLL).
     * SGD-based regressors merge approximately; they have no second-moment
     * information for the principled combine. Each stat's KDoc documents its
     * merge semantics.
     */
    fun merge(values: R)

    /**
     * Reset the stat to its prior-seeded baseline. Equivalent to constructing
     * a fresh stat with the same configuration, but in place; keeps the same
     * [Concurrency] and any per-stat tunables.
     */
    fun reset()

    /**
     * Materialise the current state as an immutable [Result]. Reads never
     * mutate, so the caller can read as often as it likes without affecting
     * the stream.
     *
     * Snapshot consistency depends on the configured [Concurrency]. Under
     * [Concurrency.Strict] / [Concurrency.HighWrite] a read locks against
     * writers so coupled cells stay consistent. Under [Concurrency.Relaxed]
     * the cells race and the snapshot may drift by ULPs of the workload
     * under heavy contention; the drift is bounded and the read never throws.
     *
     * [timestampNanos] is the read timestamp. Stats that don't care about
     * time silently drop it; stats that do (rates, decay families, recency,
     * windowed wrappers) use it as the ordering signal.
     */
    fun read(timestampNanos: Long = currentTimeNanos()): R

    /**
     * Spawn a fresh accumulator with the same configuration. Optionally
     * override the [Concurrency]; useful for materialising a wire spec at
     * a different concurrency level than the source.
     *
     * The returned stat is independent: its state starts at the configured
     * baseline, not at the source's current state. Each modality subtype
     * narrows the return type so chaining doesn't lose the modality.
     */
    fun create(concurrency: Concurrency? = null): Stat<R>
}

/**
 * Accumulator over a single scalar time series. The default modality; most
 * descriptive statistics ([MeanStat][com.eignex.kumulant.stat.summary.MeanStat],
 * [VarianceStat][com.eignex.kumulant.stat.summary.VarianceStat], the quantile
 * sketches, the rate family, the decay family) implement this shape.
 *
 * See [Stat] for the per-observation `weight` contract, which is library-wide:
 * zero is a no-op, and a negative weight downdates or throws depending on the
 * statistic.
 */
interface SeriesStat<R : Result> : Stat<R> {
    /** Record an observation with the given [weight], stamped at the current time. */
    fun update(value: Double, weight: Double = 1.0) = update(value, currentTimeNanos(), weight)

    /**
     * Record an observation at [timestampNanos] with the given [weight].
     * Stats that consume time (rates, decay, windowing) use this as the
     * ordering signal; pass a monotonic stamp when feeding from a replay log.
     */
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
    /** Record an observation with the given [weight], stamped at the current time. */
    fun update(value: Long, weight: Double = 1.0) = update(value, currentTimeNanos(), weight)

    /**
     * Record an observation at [timestampNanos] with the given [weight].
     * Time matters for rate-shaped discrete stats; for cardinality / sketch
     * stats the stamp is dropped.
     */
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
    /** Record an (x, y) observation with the given [weight] at the current time. */
    fun update(x: Double, y: Double, weight: Double = 1.0) = update(x, y, currentTimeNanos(), weight)

    /** Record an (x, y) observation at [timestampNanos] with the given [weight]. */
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
 * to a full Bayesian linear regression with covariance
 * ([BayesianRegressionStat][com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat])
 * to a non-linear decision tree
 * ([DecisionTreeRegressionStat][com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat]).
 * They share the update shape and differ in what they expose on [read].
 *
 * Inputs are passed as [VectorView] so callers can submit sparse feature
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

    /** Record an `(x, y)` observation with the given [weight] at the current time. */
    fun update(x: VectorView, y: Double, weight: Double = 1.0) = update(x, y, currentTimeNanos(), weight)

    /** Record an `(x, y)` observation at [timestampNanos] with the given [weight]. */
    fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double = 1.0)

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
 * series stat across `dimensions` channels) and for the multivariate anomaly
 * detector
 * ([HalfSpaceTreesStat][com.eignex.kumulant.stat.anomaly.HalfSpaceTreesStat]).
 *
 * Like [RegressionStat], inputs are passed as [VectorView] so sparse callers
 * don't pay for dense materialisation. The [DoubleArray] convenience
 * overloads wrap the array in a [DenseVector].
 */
interface VectorStat<R : Result> : Stat<R> {
    /** Record a [vector] observation with the given [weight] at the current time. */
    fun update(vector: VectorView, weight: Double = 1.0) = update(vector, currentTimeNanos(), weight)

    /** Record a [vector] observation at [timestampNanos] with the given [weight]. */
    fun update(vector: VectorView, timestampNanos: Long, weight: Double = 1.0)

    /** Convenience overload that wraps [vector] as a [DenseVector]. */
    fun update(vector: DoubleArray, weight: Double = 1.0) = update(DenseVector.of(vector), currentTimeNanos(), weight)

    /** Timestamped convenience overload that wraps [vector] as a [DenseVector]. */
    fun update(vector: DoubleArray, timestampNanos: Long, weight: Double = 1.0) =
        update(DenseVector.of(vector), timestampNanos, weight)

    override fun create(concurrency: Concurrency?): VectorStat<R>
}

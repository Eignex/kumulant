package com.eignex.kumulant.operation

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.IndexedResult
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.schema.expr.ScalarExpr

// withFeedback is the streaming-feature-engineering primitive: each update is first sent
// to a `primary` stat that maintains running state, then the raw value and `primary`'s
// just-updated snapshot are projected to a transformed value that is forwarded to the
// inner stat. The projection is a [ScalarExpr] AST node so the whole composition is
// wire-portable. AST nodes such as [com.eignex.kumulant.schema.expr.Center] and
// [com.eignex.kumulant.schema.expr.Scale] address fields on the primary's snapshot.
//
// Preprocessing recipes (one-liners on top of withFeedback):
//   StandardScaler: `inner.withFeedback(VarianceStat()) { (X - Center) / Scale }`
//   RobustScaler:   `inner.withFeedback(MadStat()) { (X - Center) / Scale }`
//
// The primary stat is owned by the wrapper. To inspect its snapshot directly, read it
// through the [primary] property (the wrapper exposes its primary instance).
//
// Concurrency: order-dependent cascade. Each update touches primary then inner in
// sequence; the effective model is the stricter of primary's and inner's. Under
// Relaxed the primary snapshot read between the two updates may briefly lag a racing
// writer; never throws.

/** Wrap this inner series stat with a feedback primary; the projection sees the
 *  primary's just-updated snapshot via the [ScalarExpr] AST. */
internal fun <P : Result, I : Result> SeriesStat<I>.withFeedback(
    primary: SeriesStat<P>,
    project: ScalarExpr,
): SeriesStat<I> = FeedbackSeriesStat(this, primary, project)

internal class FeedbackSeriesStat<P : Result, I : Result>(
    private val inner: SeriesStat<I>,
    /** Primary state-tracking stat owned by the wrapper. Exposed for snapshot inspection. */
    val primary: SeriesStat<P>,
    private val project: ScalarExpr,
) : SeriesStat<I>,
    Stat<I> by inner {

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        primary.update(value, timestampNanos, weight)
        val snapshot = primary.read(timestampNanos)
        inner.update(project.eval(value, 0.0, EMPTY_VECTOR, snapshot), timestampNanos, weight)
    }

    override fun reset() {
        primary.reset()
        inner.reset()
    }

    override fun create(concurrency: Concurrency?): SeriesStat<I> =
        FeedbackSeriesStat(inner.create(concurrency), primary.create(concurrency), project)

    companion object {
        private val EMPTY_VECTOR = DoubleArray(0)
    }
}

/**
 * Vector analogue of `withFeedback`: the [primary] is a fan-out stat (typically
 * [VectorizedStat]) whose `read` returns a [ResultList] of per-coordinate snapshots.
 * On each update, the input vector is sent to [primary] coordinate-by-coordinate, then
 * [project] is evaluated per coordinate against its own primary snapshot to produce
 * the transformed vector forwarded to the inner stat.
 */
internal fun <P : Result, I : Result> VectorStat<I>.withFeedback(
    primary: VectorStat<ResultList<P>>,
    project: ScalarExpr,
): VectorStat<I> = FeedbackVectorStat(this, primary, project)

internal class FeedbackVectorStat<P : Result, I : Result>(
    private val inner: VectorStat<I>,
    /** Per-coordinate primary state-tracking stat owned by the wrapper. */
    val primary: VectorStat<ResultList<P>>,
    private val project: ScalarExpr,
) : VectorStat<I>,
    Stat<I> by inner {

    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        primary.update(vector, timestampNanos, weight)
        val snapshot = primary.read(timestampNanos)
        val transformed = DoubleArray(vector.size) { i ->
            project.eval(vector[i], 0.0, EMPTY_VECTOR, IndexedResult(snapshot.results[i], i))
        }
        inner.update(transformed, timestampNanos, weight)
    }

    override fun reset() {
        primary.reset()
        inner.reset()
    }

    override fun create(concurrency: Concurrency?): VectorStat<I> =
        FeedbackVectorStat(inner.create(concurrency), primary.create(concurrency), project)

    companion object {
        private val EMPTY_VECTOR = DoubleArray(0)
    }
}

/**
 * Regression analogue of `withFeedback`: the [primary] is a per-feature fan-out
 * ([VectorizedStat]); the projection is evaluated element-wise against each
 * coordinate's primary snapshot and the transformed feature vector is forwarded to
 * the inner regressor. `y` and `weight` pass through unchanged.
 */
internal fun <P : Result, R : Result> RegressionStat<R>.withFeedback(
    primary: VectorStat<ResultList<P>>,
    project: ScalarExpr,
): RegressionStat<R> = FeedbackRegressionStat(this, primary, project)

internal class FeedbackRegressionStat<P : Result, R : Result>(
    private val inner: RegressionStat<R>,
    /** Per-coordinate primary state-tracking stat owned by the wrapper. */
    val primary: VectorStat<ResultList<P>>,
    private val project: ScalarExpr,
) : RegressionStat<R>,
    Stat<R> by inner {

    override val featureSize: Int get() = inner.featureSize

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        primary.update(x, timestampNanos, weight)
        val snapshot = primary.read(timestampNanos)
        val transformed = DoubleArray(x.size) { i ->
            project.eval(x[i], 0.0, EMPTY_VECTOR, IndexedResult(snapshot.results[i], i))
        }
        inner.update(transformed, y, timestampNanos, weight)
    }

    override fun reset() {
        primary.reset()
        inner.reset()
    }

    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        FeedbackRegressionStat(inner.create(concurrency), primary.create(concurrency), project)

    companion object {
        private val EMPTY_VECTOR = DoubleArray(0)
    }
}

/**
 * Paired analogue of `withFeedback`: separate [primaryX] and [primaryY] state-tracking
 * stats track the two axes independently. On each update, `x` is sent to `primaryX`
 * and `y` to `primaryY`; the same [project] AST is evaluated twice (once per axis,
 * each against its own primary snapshot) and the transformed `(x', y')` is forwarded
 * to the inner paired stat.
 */
internal fun <P : Result, R : Result> PairedStat<R>.withFeedback(
    primaryX: SeriesStat<P>,
    primaryY: SeriesStat<P>,
    project: ScalarExpr,
): PairedStat<R> = FeedbackPairedStat(this, primaryX, primaryY, project)

internal class FeedbackPairedStat<P : Result, R : Result>(
    private val inner: PairedStat<R>,
    /** x-axis state-tracking stat owned by the wrapper. */
    val primaryX: SeriesStat<P>,
    /** y-axis state-tracking stat owned by the wrapper. */
    val primaryY: SeriesStat<P>,
    private val project: ScalarExpr,
) : PairedStat<R>,
    Stat<R> by inner {

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        primaryX.update(x, timestampNanos, weight)
        primaryY.update(y, timestampNanos, weight)
        val tx = project.eval(x, 0.0, EMPTY_VECTOR, IndexedResult(primaryX.read(timestampNanos), 0))
        val ty = project.eval(y, 0.0, EMPTY_VECTOR, IndexedResult(primaryY.read(timestampNanos), 1))
        inner.update(tx, ty, timestampNanos, weight)
    }

    override fun reset() {
        primaryX.reset()
        primaryY.reset()
        inner.reset()
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> = FeedbackPairedStat(
        inner.create(concurrency),
        primaryX.create(concurrency),
        primaryY.create(concurrency),
        project,
    )

    companion object {
        private val EMPTY_VECTOR = DoubleArray(0)
    }
}

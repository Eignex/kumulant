package com.eignex.kumulant.operation

import com.eignex.koblas.Vector
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
import kotlin.time.Duration

// Each update reaches the primary first; the projection uses its just-updated snapshot.
// Windowing applies to the inner stat, keeping the primary distribution over the whole stream.
// Concurrent updates may interleave between the primary update, snapshot, and inner update.

// The projection sees the primary snapshot after its update.
internal fun <P : Result, I : Result> SeriesStat<I>.withFeedback(
    primary: SeriesStat<P>,
    project: ScalarExpr,
): SeriesStat<I> = FeedbackSeriesStat(this, primary, project)

internal class FeedbackSeriesStat<P : Result, I : Result>(
    private val inner: SeriesStat<I>,
    val primary: SeriesStat<P>,
    private val project: ScalarExpr,
) : SeriesStat<I>,
    WindowsInside<I, SeriesStat<I>>,
    Stat<I> by inner {

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        primary.update(value, timestampNanos, weight)
        val snapshot = primary.read(timestampNanos)
        inner.update(project.eval(value, primary = snapshot), timestampNanos, weight)
    }

    override fun reset() {
        primary.reset()
        inner.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<I> =
        FeedbackSeriesStat(inner.windowed(duration, slices, concurrency), primary, project)

    override fun create(concurrency: Concurrency?): SeriesStat<I> =
        FeedbackSeriesStat(inner.create(concurrency), primary.create(concurrency), project)
}

// Each coordinate is projected against its own just-updated primary snapshot.
internal fun <P : Result, I : Result> VectorStat<I>.withFeedback(
    primary: VectorStat<ResultList<P>>,
    project: ScalarExpr,
): VectorStat<I> = FeedbackVectorStat(this, primary, project)

internal class FeedbackVectorStat<P : Result, I : Result>(
    private val inner: VectorStat<I>,
    val primary: VectorStat<ResultList<P>>,
    private val project: ScalarExpr,
) : VectorStat<I>,
    WindowsInside<I, VectorStat<I>>,
    Stat<I> by inner {

    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        primary.update(vector, timestampNanos, weight)
        val snapshot = primary.read(timestampNanos)
        val transformed = DoubleArray(vector.size) { i ->
            project.eval(vector[i], primary = IndexedResult(snapshot.results[i], i))
        }
        inner.update(transformed, timestampNanos, weight)
    }

    override fun reset() {
        primary.reset()
        inner.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<I> =
        FeedbackVectorStat(inner.windowed(duration, slices, concurrency), primary, project)

    override fun create(concurrency: Concurrency?): VectorStat<I> =
        FeedbackVectorStat(inner.create(concurrency), primary.create(concurrency), project)
}

// Each feature is projected against its own just-updated primary snapshot; y and weight pass through.
internal fun <P : Result, R : Result> RegressionStat<R>.withFeedback(
    primary: VectorStat<ResultList<P>>,
    project: ScalarExpr,
): RegressionStat<R> = FeedbackRegressionStat(this, primary, project)

internal class FeedbackRegressionStat<P : Result, R : Result>(
    private val inner: RegressionStat<R>,
    val primary: VectorStat<ResultList<P>>,
    private val project: ScalarExpr,
) : RegressionStat<R>,
    Stat<R> by inner {

    override val featureSize: Int get() = inner.featureSize

    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        primary.update(x, timestampNanos, weight)
        val snapshot = primary.read(timestampNanos)
        val transformed = DoubleArray(x.size) { i ->
            project.eval(x[i], primary = IndexedResult(snapshot.results[i], i))
        }
        inner.update(transformed, y, timestampNanos, weight)
    }

    override fun reset() {
        primary.reset()
        inner.reset()
    }

    override fun create(concurrency: Concurrency?): RegressionStat<R> =
        FeedbackRegressionStat(inner.create(concurrency), primary.create(concurrency), project)
}

// Each axis is projected against its own just-updated primary snapshot.
internal fun <P : Result, R : Result> PairedStat<R>.withFeedback(
    primaryX: SeriesStat<P>,
    primaryY: SeriesStat<P>,
    project: ScalarExpr,
): PairedStat<R> = FeedbackPairedStat(this, primaryX, primaryY, project)

internal class FeedbackPairedStat<P : Result, R : Result>(
    private val inner: PairedStat<R>,
    val primaryX: SeriesStat<P>,
    val primaryY: SeriesStat<P>,
    private val project: ScalarExpr,
) : PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by inner {

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        primaryX.update(x, timestampNanos, weight)
        primaryY.update(y, timestampNanos, weight)
        val tx = project.eval(x, primary = IndexedResult(primaryX.read(timestampNanos), 0))
        val ty = project.eval(y, primary = IndexedResult(primaryY.read(timestampNanos), 1))
        inner.update(tx, ty, timestampNanos, weight)
    }

    override fun reset() {
        primaryX.reset()
        primaryY.reset()
        inner.reset()
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        FeedbackPairedStat(inner.windowed(duration, slices, concurrency), primaryX, primaryY, project)

    override fun create(concurrency: Concurrency?): PairedStat<R> = FeedbackPairedStat(
        inner.create(concurrency),
        primaryX.create(concurrency),
        primaryY.create(concurrency),
        project,
    )
}

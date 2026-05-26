package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.schema.ScalarExpr

// withFeedback is the streaming-feature-engineering primitive: each update is first sent
// to a `primary` stat that maintains running state, then the raw value and `primary`'s
// just-updated snapshot are projected to a transformed value that is forwarded to the
// inner stat. The projection is a [ScalarExpr] AST node so the whole composition is
// wire-portable. AST nodes such as [com.eignex.kumulant.schema.Center] and
// [com.eignex.kumulant.schema.Scale] address fields on the primary's snapshot.
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
fun <P : Result, I : Result> SeriesStat<I>.withFeedback(primary: SeriesStat<P>, project: ScalarExpr): SeriesStat<I> =
    FeedbackSeriesStat(this, primary, project)

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
fun <P : Result, I : Result> VectorStat<I>.withFeedback(
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
            project.eval(vector[i], 0.0, EMPTY_VECTOR, snapshot.results[i])
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

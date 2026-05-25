package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// band derives the center / scale / lower / upper bounds from any inner stat whose
// result implements [HasCenterScale]. The wrapper forwards update/reset/create to the
// delegate; read projects to [BandResult]. Merge through the wrapper is intentionally
// unsupported - merge the inner stat directly when combining replicas.

/** Center plus a configurable multiple of scale, derived from a [HasCenterScale] result. */
@Serializable
@SerialName("BandResult")
data class BandResult(
    /** Center exposed by the inner result. */
    val center: Double,
    /** Scale exposed by the inner result. */
    val scale: Double,
    /** Multiplier applied to [scale] when computing [lower] and [upper]. */
    val k: Double,
    /** `center - k * scale`. */
    val lower: Double,
    /** `center + k * scale`. */
    val upper: Double,
) : Result

/** Wrap this series stat to expose a `[lower, upper]` band of width [k] * scale around center. */
fun <R> SeriesStat<R>.band(k: Double): SeriesStat<BandResult>
    where R : HasCenterScale = BandSeriesStat(this, k)

internal class BandSeriesStat<R>(private val delegate: SeriesStat<R>, private val k: Double) :
    SeriesStat<BandResult> where R : HasCenterScale {

    override val concurrency: Concurrency get() = delegate.concurrency

    override fun update(value: Double, timestampNanos: Long, weight: Double) =
        delegate.update(value, timestampNanos, weight)

    override fun read(timestampNanos: Long): BandResult {
        val r = delegate.read(timestampNanos)
        val c = r.center
        val s = r.scale
        return BandResult(center = c, scale = s, k = k, lower = c - k * s, upper = c + k * s)
    }

    override fun merge(values: BandResult): Unit =
        error("band wrapper cannot merge BandResult; merge the inner stat directly")

    override fun reset() = delegate.reset()

    override fun create(concurrency: Concurrency?): SeriesStat<BandResult> =
        BandSeriesStat(delegate.create(concurrency), k)
}

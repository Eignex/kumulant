package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireLiveWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Arithmetic mean. */
@Serializable
@SerialName("MeanResult")
data class MeanResult(
    /** Running arithmetic mean. */
    val mean: Double,
) : Result

/** Weighted mean and accumulated weight. */
@Serializable
@SerialName("WeightedMeanResult")
data class WeightedMeanResult(
    /** Cumulative observation weight folded in. */
    override val totalWeights: Double,
    /** Weighted running mean. */
    val mean: Double,
) : HasObservationCount

/**
 * Weighted arithmetic mean via Welford-style online update.
 *
 * Numerically stable across wide dynamic ranges; merges two [MeanStat]s using
 * Chan's parallel algorithm.
 *
 * **Use cases:** central-tendency monitoring on any scalar quantity. Compose
 * with [withValue][com.eignex.kumulant.schema.ops.withValue] /
 * [withWeight][com.eignex.kumulant.schema.ops.withWeight] on the [Mean]
 * [spec][com.eignex.kumulant.schema.spec.Mean] to derive other means (event rate,
 * conditional mean, etc.), then `materialize()` the composed spec.
 *
 * **Weights:** zero is a no-op. A negative weight is a downdate: it removes an
 * observation previously folded in, inverting the update exactly, which is how a
 * caller drives a sliding window by hand. The one rejected case is a downdate that
 * would take the accumulated weight to zero or below, since every step divides by
 * the new total and the accumulator would be left permanently non-finite; that
 * throws [IllegalArgumentException].
 *
 * **Memory:** O(1); two doubles plus a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Welford-coupled cells. [Concurrency.Strict] and
 * [Concurrency.HighWrite] lock the body; exact match to a serial run up to
 * floating-point reorder ULPs. [Concurrency.Relaxed] drops the lock; the
 * coupled `(totalWeights, mean)` pair can drift by ~1e-5 relative under
 * contention but never throws.
 */
class MeanStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<WeightedMeanResult> {

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            requireLiveWeight(totalWeights.load(), weight)
            val nextW = totalWeights.addAndGet(weight)
            val delta = value - mean.load()
            mean.add(delta * (weight / nextW))
        }
    }

    override fun read(timestampNanos: Long): WeightedMeanResult = lock.guarded {
        WeightedMeanResult(totalWeights.load(), mean.load())
    }

    override fun merge(values: WeightedMeanResult, workspace: com.eignex.koblas.Workspace?) = lock.guarded {
        if (values.totalWeights <= 0.0) return@guarded
        val nextW = totalWeights.addAndGet(values.totalWeights)
        val delta = values.mean - mean.load()
        mean.add(delta * (values.totalWeights / nextW))
    }

    override fun reset() = lock.guarded {
        totalWeights.store(0.0)
        mean.store(0.0)
    }

    override fun create(concurrency: Concurrency?) = MeanStat(concurrency ?: this.concurrency)
}

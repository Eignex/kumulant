package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.asin
import kotlin.math.max

/**
 * T-digest snapshot: [means]/[weights] are the centroid arrays sorted by mean,
 * with [quantiles] precomputed for [probabilities] via CDF inversion.
 */
@Serializable
@SerialName("TDigest")
data class TDigestResult(
    val probabilities: DoubleArray,
    val quantiles: DoubleArray,
    val means: DoubleArray,
    val weights: DoubleArray,
    val totalWeight: Double,
    val compression: Double
) : Result

/** Convert centroids to a sparse histogram with bins centered on each centroid. */
fun TDigestResult.toSparseHistogram(): SparseHistogramResult {
    val n = means.size
    if (n == 0) return SparseHistogramResult(DoubleArray(0), DoubleArray(0), DoubleArray(0))
    if (n == 1) {
        return SparseHistogramResult(
            doubleArrayOf(means[0]),
            doubleArrayOf(means[0]),
            doubleArrayOf(weights[0])
        )
    }
    val lowers = DoubleArray(n)
    val uppers = DoubleArray(n)
    for (i in 0 until n) {
        val left = if (i == 0) means[0] else (means[i - 1] + means[i]) / 2.0
        val right = if (i == n - 1) means[n - 1] else (means[i] + means[i + 1]) / 2.0
        lowers[i] = left
        uppers[i] = right
    }
    return SparseHistogramResult(lowers, uppers, weights.copyOf())
}

/**
 * Buffered merging T-Digest (Dunning) with `k1` scaling function for high-fidelity
 * extreme-quantile estimates and bounded centroid count. [compression] (δ) caps
 * centroids to roughly `~6·δ`.
 *
 * Updates buffer values until [bufferCap] is reached, then fold them into the
 * sorted centroid list under the `k1`-difference ≤ 1 merge rule. State is
 * CAS-swapped immutables so SerialMode is cheap and AtomicMode is correct
 * (though not lock-free under heavy contention).
 */
class TDigest(
    val compression: Double = 100.0,
    val probabilities: DoubleArray = doubleArrayOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<TDigestResult> {

    init {
        require(compression > 0.0) { "compression must be > 0" }
        for (p in probabilities) {
            require(p in 0.0..1.0) { "probabilities must be in [0,1]" }
        }
    }

    private val bufferCap: Int = max(10, (5.0 * compression).toInt())

    private class State(
        val means: DoubleArray,
        val weights: DoubleArray,
        val totalWeight: Double,
        val buffer: DoubleArray,
        val bufferWeights: DoubleArray,
        val bufferLen: Int
    )

    private fun emptyState() = State(
        DoubleArray(0),
        DoubleArray(0),
        0.0,
        DoubleArray(bufferCap),
        DoubleArray(bufferCap),
        0
    )

    private val stateRef = mode.newReference(emptyState())

    private fun k1(q: Double): Double =
        compression / (2.0 * PI) * asin(2.0 * q.coerceIn(0.0, 1.0) - 1.0)

    private fun compress(s: State): State {
        if (s.bufferLen == 0 && s.means.isEmpty()) return s

        val n = s.means.size + s.bufferLen
        val combinedM = DoubleArray(n)
        val combinedW = DoubleArray(n)

        var i = 0
        var j = 0
        var c = 0
        val bufIdx = (0 until s.bufferLen).sortedBy { s.buffer[it] }
        while (i < s.means.size && j < s.bufferLen) {
            val bv = s.buffer[bufIdx[j]]
            if (s.means[i] <= bv) {
                combinedM[c] = s.means[i]
                combinedW[c] = s.weights[i]
                i++
            } else {
                combinedM[c] = bv
                combinedW[c] = s.bufferWeights[bufIdx[j]]
                j++
            }
            c++
        }
        while (i < s.means.size) {
            combinedM[c] = s.means[i]
            combinedW[c] = s.weights[i]
            i++; c++
        }
        while (j < s.bufferLen) {
            combinedM[c] = s.buffer[bufIdx[j]]
            combinedW[c] = s.bufferWeights[bufIdx[j]]
            j++; c++
        }

        val totalWeight = s.totalWeight
        if (totalWeight <= 0.0) return emptyState()

        val outM = DoubleArray(n)
        val outW = DoubleArray(n)
        var outLen = 0

        var curM = combinedM[0]
        var curW = combinedW[0]
        var qLeft = 0.0
        var kLeft = k1(qLeft)

        for (idx in 1 until n) {
            val nextM = combinedM[idx]
            val nextW = combinedW[idx]
            val combinedQRight = qLeft + (curW + nextW) / totalWeight
            val kRight = k1(combinedQRight)
            if (kRight - kLeft <= 1.0) {
                val mergedW = curW + nextW
                curM = curM + (nextM - curM) * nextW / mergedW
                curW = mergedW
            } else {
                outM[outLen] = curM
                outW[outLen] = curW
                outLen++
                qLeft += curW / totalWeight
                kLeft = k1(qLeft)
                curM = nextM
                curW = nextW
            }
        }
        outM[outLen] = curM
        outW[outLen] = curW
        outLen++

        return State(
            outM.copyOf(outLen),
            outW.copyOf(outLen),
            totalWeight,
            DoubleArray(bufferCap),
            DoubleArray(bufferCap),
            0
        )
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0 || value.isNaN()) return

        while (true) {
            val s = stateRef.load()
            val newBuf = s.buffer.copyOf()
            val newBufW = s.bufferWeights.copyOf()
            newBuf[s.bufferLen] = value
            newBufW[s.bufferLen] = weight
            val staged = State(
                s.means,
                s.weights,
                s.totalWeight + weight,
                newBuf,
                newBufW,
                s.bufferLen + 1
            )
            val finalState = if (staged.bufferLen >= bufferCap) compress(staged) else staged
            if (stateRef.compareAndSet(s, finalState)) return
        }
    }

    override fun create(mode: StreamMode?) = TDigest(
        compression,
        probabilities,
        mode ?: this.mode
    )

    override fun merge(values: TDigestResult) {
        require(abs(compression - values.compression) < 1e-9) {
            "Cannot merge TDigests with different compression"
        }
        for (i in values.means.indices) {
            update(values.means[i], 0L, values.weights[i])
        }
    }

    override fun reset() {
        stateRef.store(emptyState())
    }

    override fun read(timestampNanos: Long): TDigestResult {
        while (true) {
            val s = stateRef.load()
            if (s.bufferLen == 0) break
            val compressed = compress(s)
            if (stateRef.compareAndSet(s, compressed)) break
        }

        val s = stateRef.load()
        val means = s.means
        val weights = s.weights
        val total = s.totalWeight

        val computed = DoubleArray(probabilities.size)
        if (means.isEmpty() || total <= 0.0) {
            return TDigestResult(probabilities, computed, means.copyOf(), weights.copyOf(), total, compression)
        }

        // Cumulative rank at each centroid's center (half-weight offsets).
        val centers = DoubleArray(means.size)
        var acc = 0.0
        for (i in means.indices) {
            centers[i] = acc + weights[i] / 2.0
            acc += weights[i]
        }

        for (pi in probabilities.indices) {
            val targetRank = probabilities[pi] * total
            val n = means.size
            val q: Double
            if (n == 1 || targetRank <= centers[0]) {
                q = means[0]
            } else if (targetRank >= centers[n - 1]) {
                q = means[n - 1]
            } else {
                var idx = 0
                for (i in 0 until n - 1) {
                    if (targetRank <= centers[i + 1]) {
                        idx = i; break
                    }
                }
                val span = centers[idx + 1] - centers[idx]
                val frac = if (span <= 0.0) 0.0 else (targetRank - centers[idx]) / span
                q = means[idx] + frac * (means[idx + 1] - means[idx])
            }
            computed[pi] = q
        }

        return TDigestResult(
            probabilities = probabilities,
            quantiles = computed,
            means = means.copyOf(),
            weights = weights.copyOf(),
            totalWeight = total,
            compression = compression
        )
    }
}

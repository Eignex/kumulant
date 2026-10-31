package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.preview
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock
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
@SerialName("TDigestResult")
data class TDigestResult(
    /** Probabilities at which [quantiles] are evaluated; parallel to [quantiles]. */
    val probabilities: DoubleArray,
    /** Estimated quantile values, parallel to [probabilities]. */
    val quantiles: DoubleArray,
    /** Centroid means sorted ascending; parallel to [weights]. */
    val means: DoubleArray,
    /** Centroid weights, parallel to [means]. */
    val weights: DoubleArray,
    /** Cumulative observation weight folded in. */
    val totalWeight: Double,
    /** T-digest compression parameter; lower = more centroids, tighter quantiles. */
    val compression: Double,
) : HasObservationCount {

    override val totalWeights: Double get() = totalWeight
    override fun equals(other: Any?): Boolean = other is TDigestResult &&
        probabilities.contentEquals(other.probabilities) &&
        quantiles.contentEquals(other.quantiles) &&
        means.contentEquals(other.means) &&
        weights.contentEquals(other.weights) &&
        totalWeight == other.totalWeight &&
        compression == other.compression

    override fun hashCode(): Int {
        var h = probabilities.contentHashCode()
        h = 31 * h + quantiles.contentHashCode()
        h = 31 * h + means.contentHashCode()
        h = 31 * h + weights.contentHashCode()
        h = 31 * h + totalWeight.hashCode()
        h = 31 * h + compression.hashCode()
        return h
    }

    override fun toString(): String = "TDigestResult(" +
        "probabilities=${probabilities.preview()}, " +
        "quantiles=${quantiles.preview()}, " +
        "means=${means.preview()}, " +
        "weights=${weights.preview()}, " +
        "totalWeight=$totalWeight, " +
        "compression=$compression)"
}

/** Convert centroids to a sparse histogram with bins centered on each centroid. */
fun TDigestResult.toSparseHistogram(): SparseHistogramResult {
    val n = means.size
    if (n == 0) return SparseHistogramResult(DoubleArray(0), DoubleArray(0), DoubleArray(0))
    if (n == 1) {
        return SparseHistogramResult(
            doubleArrayOf(means[0]),
            doubleArrayOf(means[0]),
            doubleArrayOf(weights[0]),
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
 * extreme-quantile estimates and bounded centroid count. [compression] (delta) caps
 * centroids to roughly `~6*delta`.
 *
 * Updates buffer values until the internal `bufferCap` is reached, then fold them into
 * the sorted centroid list under the `k1`-difference ≤ 1 merge rule.
 *
 * **Use cases:** approximate percentile estimation with adaptive resolution;
 * tighter near the tails (0.001 / 0.999 percentiles), looser in the middle.
 * Reach for this over [DDSketchStat] when you want extreme-quantile accuracy
 * without committing to a relative-error parameter.
 *
 * **Memory:** O([compression]) centroids (~6 · delta) plus a fixed-size buffer.
 *
 * **Update:** O(1) amortised per observation, with a periodic
 * O([compression] · log [compression]) compress when the buffer fills.
 *
 * **Concurrency:** Body locked under any concurrent level. One lock protects buffer
 * appends, compression, reads, merges, and resets, so observations cannot cross a
 * buffer drain before their value and weight are stored. [Concurrency.None] runs
 * without synchronisation. Centroid compression remains approximate and depends
 * on update order.
 */
class TDigestStat(
    /** Compression parameter; higher permits more centroids and finer quantile resolution. */
    val compression: Double = StatDefaults.TDIGEST_COMPRESSION,
    /** Quantiles to evaluate at read time. */
    val probabilities: DoubleArray = StatDefaults.quantileProbabilities(),
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<TDigestResult> {

    init {
        require(compression > 0.0) { "compression must be > 0" }
        for (p in probabilities) {
            require(p in 0.0..1.0) { "probabilities must be in [0,1]" }
        }
    }

    private val bufferCap: Int = max(10, (5.0 * compression).toInt())
    private val lock = concurrency.serializedLock()
    private var bufferSize = 0
    private val buffer = DoubleArray(bufferCap)
    private val bufferWeights = DoubleArray(bufferCap)
    private var totalWeight = 0.0
    private var means = DoubleArray(0)
    private var weights = DoubleArray(0)

    private fun k1(q: Double): Double = compression / (2.0 * PI) * asin(2.0 * q.coerceIn(0.0, 1.0) - 1.0)

    private fun drainLocked() {
        val size = bufferSize
        if (size == 0) return
        var bufferedWeight = 0.0
        for (i in 0 until size) bufferedWeight += bufferWeights[i]
        totalWeight += bufferedWeight
        compressInto(buffer, bufferWeights, size)
        bufferSize = 0
    }

    // Scratch buffers for the buffer-ordering sort, reused across compressions. Only ever
    // touched under the lock, so no synchronisation of their own is needed.
    private var sortIdx: IntArray = IntArray(0)
    private var sortTmp: IntArray = IntArray(0)

    // Bottom-up merge sort of the buffer indices keyed on `bufVal`: stable, `O(n log n)`, and
    // allocation-free after the first call. A comparator sort over an index list would box every
    // index (~8 KiB per flush at the default compression), and an insertion sort would be `O(n^2)`
    // over a 500-entry buffer.
    private fun sortedBufferIndices(bufVal: DoubleArray, bufLen: Int): IntArray {
        if (sortIdx.size < bufLen) {
            sortIdx = IntArray(bufLen)
            sortTmp = IntArray(bufLen)
        }
        val idx = sortIdx
        for (i in 0 until bufLen) idx[i] = i
        var width = 1
        while (width < bufLen) {
            var lo = 0
            while (lo < bufLen) {
                val mid = minOf(lo + width, bufLen)
                val hi = minOf(lo + 2 * width, bufLen)
                var a = lo
                var b = mid
                var k = lo
                while (a < mid && b < hi) {
                    sortTmp[k++] = if (bufVal[idx[a]] <= bufVal[idx[b]]) idx[a++] else idx[b++]
                }
                while (a < mid) sortTmp[k++] = idx[a++]
                while (b < hi) sortTmp[k++] = idx[b++]
                lo += 2 * width
            }
            sortTmp.copyInto(idx, 0, 0, bufLen)
            width *= 2
        }
        return idx
    }

    private fun compressInto(bufVal: DoubleArray, bufW: DoubleArray, bufLen: Int) {
        if (bufLen == 0 && means.isEmpty()) return

        val n = means.size + bufLen
        val combinedM = DoubleArray(n)
        val combinedW = DoubleArray(n)

        var i = 0
        var j = 0
        var c = 0
        val bufIdx = sortedBufferIndices(bufVal, bufLen)
        while (i < means.size && j < bufLen) {
            val bv = bufVal[bufIdx[j]]
            if (means[i] <= bv) {
                combinedM[c] = means[i]
                combinedW[c] = weights[i]
                i++
            } else {
                combinedM[c] = bv
                combinedW[c] = bufW[bufIdx[j]]
                j++
            }
            c++
        }
        while (i < means.size) {
            combinedM[c] = means[i]
            combinedW[c] = weights[i]
            i++
            c++
        }
        while (j < bufLen) {
            combinedM[c] = bufVal[bufIdx[j]]
            combinedW[c] = bufW[bufIdx[j]]
            j++
            c++
        }

        val total = totalWeight
        if (total <= 0.0) {
            means = DoubleArray(0)
            weights = DoubleArray(0)
            return
        }

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
            val combinedQRight = qLeft + (curW + nextW) / total
            val kRight = k1(combinedQRight)
            // An infinite mean only merges with its own value: the weighted step would compute
            // Inf - Inf, or -Inf + Inf, and leave a NaN centroid that breaks the sorted means.
            val mergeable = nextM == curM || (curM.isFinite() && nextM.isFinite())
            if (mergeable && kRight - kLeft <= 1.0) {
                val mergedW = curW + nextW
                if (nextM != curM) curM += (nextM - curM) * nextW / mergedW
                curW = mergedW
            } else {
                outM[outLen] = curM
                outW[outLen] = curW
                outLen++
                qLeft += curW / total
                kLeft = k1(qLeft)
                curM = nextM
                curW = nextW
            }
        }
        outM[outLen] = curM
        outW[outLen] = curW
        outLen++

        means = outM.copyOf(outLen)
        weights = outW.copyOf(outLen)
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            buffer[bufferSize] = value
            bufferWeights[bufferSize] = weight
            bufferSize++
            if (bufferSize == bufferCap) drainLocked()
        }
    }

    override fun create(concurrency: Concurrency?) = TDigestStat(
        compression,
        probabilities.copyOf(),
        concurrency ?: this.concurrency,
    )

    override fun validateMerge(values: TDigestResult) {
        require(abs(compression - values.compression) < PARAMETER_MATCH_TOLERANCE) {
            "Cannot merge TDigests with different compression"
        }
    }

    override fun merge(values: TDigestResult, timestampNanos: Long) {
        validateMerge(values)
        lock.guarded {
            drainLocked()
            val n = values.means.size
            val sv = DoubleArray(n)
            val sw = DoubleArray(n)
            var c = 0
            for (i in 0 until n) {
                val weight = values.weights[i]
                if (weight > 0.0 && !values.means[i].isNaN()) {
                    sv[c] = values.means[i]
                    sw[c] = weight
                    c++
                    totalWeight += weight
                }
            }
            if (c > 0) compressInto(sv, sw, c)
        }
    }

    override fun reset() = lock.guarded {
        bufferSize = 0
        totalWeight = 0.0
        means = DoubleArray(0)
        weights = DoubleArray(0)
    }

    override fun read(timestampNanos: Long): TDigestResult = lock.guarded {
        drainLocked()

        val total = totalWeight
        val computed = DoubleArray(probabilities.size)
        if (means.isEmpty() || total <= 0.0) {
            // NaN per quantile rather than 0.0, matching DDSketchStat. A zero-filled array
            // cannot be told apart from a digest that genuinely observed zeros, so an untouched
            // digest would report a p99 of 0.0 and read as healthy. `isEmpty` on the result is
            // the check for callers who would rather branch.
            computed.fill(Double.NaN)
            return@guarded TDigestResult(
                probabilities.copyOf(),
                computed,
                means.copyOf(),
                weights.copyOf(),
                total,
                compression,
            )
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
                        idx = i
                        break
                    }
                }
                val span = centers[idx + 1] - centers[idx]
                val frac = if (span <= 0.0) 0.0 else (targetRank - centers[idx]) / span
                val lo = means[idx]
                val hi = means[idx + 1]
                q = when {
                    lo == hi -> lo

                    // No interpolation toward an infinite centroid; take the nearer one.
                    !lo.isFinite() || !hi.isFinite() -> if (frac < 0.5) lo else hi

                    else -> lo + frac * (hi - lo)
                }
            }
            computed[pi] = q
        }

        // See DDSketchStat.read on why this is copied.
        TDigestResult(
            probabilities = probabilities.copyOf(),
            quantiles = computed,
            means = means.copyOf(),
            weights = weights.copyOf(),
            totalWeight = total,
            compression = compression,
        )
    }
}

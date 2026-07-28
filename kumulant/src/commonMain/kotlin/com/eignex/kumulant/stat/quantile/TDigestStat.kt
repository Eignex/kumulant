package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.preview
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock
import com.eignex.kumulant.stream.spinHint
import com.eignex.kumulant.stream.welfordLock
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
 * **Use cases:** approximate percentile estimation with adaptive resolution
 *; tighter near the tails (0.001 / 0.999 percentiles), looser in the middle.
 * Reach for this over [DDSketchStat] when you want extreme-quantile accuracy
 * without committing to a relative-error parameter, and over
 * [HdrHistogramStat] when the value range isn't known in advance.
 *
 * **Memory:** O([compression]) centroids (~6 · delta) plus a fixed-size buffer.
 *
 * **Update:** O(1) amortised per observation; a single atomic claim into the
 * buffer, with a periodic O([compression] · log [compression]) compress when
 * the buffer fills.
 *
 * **Concurrency:** Hot-path `update` is lock-free under [Concurrency.Relaxed]
 * (atomic claim into a ring buffer); a buffer-full triggers a brief locked
 * compress that does not block concurrent claims in the next epoch. Under
 * [Concurrency.Strict] / [Concurrency.HighWrite] an outer lock serialises
 * updates against reads/merges. [Concurrency.None] runs without
 * synchronisation.
 */
class TDigestStat(
    /** Compression parameter; lower = more centroids, tighter quantiles, more memory. */
    val compression: Double = 100.0,
    /** Quantiles to evaluate at read time. */
    val probabilities: DoubleArray = doubleArrayOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<TDigestResult> {

    init {
        require(compression > 0.0) { "compression must be > 0" }
        for (p in probabilities) {
            require(p in 0.0..1.0) { "probabilities must be in [0,1]" }
        }
    }

    private val bufferCap: Int = max(10, (5.0 * compression).toInt())
    private val bufferCapLong: Long = bufferCap.toLong()

    // Use monotonicMode (SerialMode under None, AtomicMode otherwise) so the buffer
    // index supports addAndGet + compareAndSet under every concurrent mode. Striped
    // adders (HighWrite) don't expose CAS and have non-linearizable reads, which
    // would break the claim/commit protocol below.
    private val mode = concurrency.monotonicMode()

    // Outer lock: noop under None/Relaxed; real under Strict/HighWrite. Linearizes
    // update/merge/read against each other for strict semantics.
    private val outerLock = concurrency.welfordLock()

    // Compress lock: noop under None; real otherwise. Protects centroid arrays and
    // the buffer-drain critical section against concurrent compress / read / merge.
    private val compressLock = concurrency.serializedLock()

    // Lock-free ring buffer (one epoch). Updates atomically claim a slot via
    // bufferIndex.addAndGet, then write value+weight and bump commitIndex. Overflow
    // (claimed > bufferCap) is funneled through compressLock to drain + reset.
    private val bufferIndex = mode.newLong(0L)
    private val commitIndex = mode.newLong(0L)
    private val buffer = mode.newDoubleArray(bufferCap)
    private val bufferWeights = mode.newDoubleArray(bufferCap)
    private val totalWeightCell = mode.newDouble(0.0)

    // Centroid arrays. Always accessed under compressLock (except trivially under None).
    private var means = DoubleArray(0)
    private var weights = DoubleArray(0)

    private fun k1(q: Double): Double = compression / (2.0 * PI) * asin(2.0 * q.coerceIn(0.0, 1.0) - 1.0)

    /**
     * Drain the current buffer epoch into the centroid arrays. Caller MUST hold
     * [compressLock]. Idempotent: a no-op if the buffer is empty.
     */
    private fun drainLocked() {
        // Freeze the buffer epoch by CAS-bumping bufferIndex to bufferCap; any concurrent
        // claimer that observes bufferIndex >= bufferCap will overflow into compressLock
        // and retry against the next epoch.
        val claimed = claimBufferEpoch() ?: return
        // Wait for in-flight commits to land. Under Relaxed an addAndGet that
        // straddles a concurrent reset can leave a stranded claim whose commit
        // never lands in this epoch's commitIndex; fall back to whatever is
        // committed so far rather than spinning forever. Drift is the Relaxed
        // contract; livelock is not.
        var committed = commitIndex.load().toInt()
        var spins = 0
        while (committed < claimed) {
            spinHint()
            committed = commitIndex.load().toInt()
            if (++spins >= SPIN_MAX) {
                // Stranded claim: trim to what we can prove committed and proceed.
                break
            }
        }
        val drained = if (committed < claimed) committed else claimed

        val snapVal = DoubleArray(drained) { buffer.load(it) }
        val snapW = DoubleArray(drained) { bufferWeights.load(it) }

        // Reset for the next epoch BEFORE compressing; new claimers can start writing
        // into a fresh buffer while we merge the snapshot.
        bufferIndex.store(0L)
        commitIndex.store(0L)

        compressInto(snapVal, snapW, drained)
    }

    private companion object {
        // Spin loop iterations before giving up on a stranded in-flight commit.
        // Sized to cover normal scheduler hiccups but small enough that an
        // orphaned claim (writer suspended across a drain reset, never lands its
        // commit in this epoch's counter) doesn't wedge the reader for long.
        private const val SPIN_MAX = 10_000
    }

    /** CAS the buffer index up to [bufferCap] to freeze the current epoch. Returns the
     *  number of valid entries claimed, or `null` if the buffer was empty. */
    private fun claimBufferEpoch(): Int? {
        while (true) {
            val cur = bufferIndex.load()
            if (cur >= bufferCapLong) return bufferCap
            if (cur == 0L) return null
            if (bufferIndex.compareAndSet(cur, bufferCapLong)) return cur.toInt()
        }
    }

    // Scratch buffers for the buffer-ordering sort, reused across compressions. Only ever
    // touched under compressLock, so no synchronisation of their own is needed.
    private var sortIdx: IntArray = IntArray(0)
    private var sortTmp: IntArray = IntArray(0)

    /**
     * Indices `0 until bufLen` ordered by `bufVal`, without boxing.
     *
     * This replaced `(0 until bufLen).sortedBy { bufVal[it] }`, which materialised a
     * `List<Integer>` of `bufLen` boxed indices plus an ArrayList and ran a comparator sort.
     * At the default compression the buffer holds 500 entries, so every flush allocated
     * roughly 8 KiB of boxes; amortised over the updates that filled it, TDigest measured
     * 82 B/op on the update path.
     *
     * Bottom-up merge sort keyed on `bufVal`. Stable, `O(n log n)`, and allocation-free after
     * the first call, which matters because an insertion sort would be `O(n^2)` over a
     * 500-entry buffer and cost more than the boxing it replaces.
     */
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

    /** Merge the snapshot into [means]/[weights] under the k1 difference rule. */
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

        val total = totalWeightCell.load()
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
            if (kRight - kLeft <= 1.0) {
                val mergedW = curW + nextW
                curM = curM + (nextM - curM) * nextW / mergedW
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
        if (weight <= 0.0 || value.isNaN()) return
        outerLock.guarded {
            while (true) {
                val claimed = bufferIndex.addAndGet(1L)
                val idx = (claimed - 1L).toInt()
                if (claimed <= bufferCapLong) {
                    buffer.store(idx, value)
                    bufferWeights.store(idx, weight)
                    totalWeightCell.add(weight)
                    commitIndex.add(1L)
                    if (claimed == bufferCapLong) {
                        compressLock.guarded { drainLocked() }
                    }
                    return@guarded
                }
                // Overflow: a concurrent compress is needed before our claim can land.
                compressLock.guarded { drainLocked() }
                // Loop and retry against the next epoch.
            }
        }
    }

    override fun create(concurrency: Concurrency?) = TDigestStat(
        compression,
        probabilities,
        concurrency ?: this.concurrency,
    )

    override fun merge(values: TDigestResult) {
        require(abs(compression - values.compression) < 1e-9) {
            "Cannot merge TDigests with different compression"
        }
        outerLock.guarded {
            compressLock.guarded {
                // Drain any buffered points first so they appear in the centroid array.
                drainLocked()
                // Feed the incoming centroids through the same compress path.
                val n = values.means.size
                val sv = DoubleArray(n)
                val sw = DoubleArray(n)
                var c = 0
                for (i in 0 until n) {
                    val w = values.weights[i]
                    if (w > 0.0 && !values.means[i].isNaN()) {
                        sv[c] = values.means[i]
                        sw[c] = w
                        c++
                        totalWeightCell.add(w)
                    }
                }
                if (c > 0) compressInto(sv, sw, c)
            }
        }
    }

    override fun reset() {
        outerLock.guarded {
            compressLock.guarded {
                bufferIndex.store(0L)
                commitIndex.store(0L)
                totalWeightCell.store(0.0)
                means = DoubleArray(0)
                weights = DoubleArray(0)
            }
        }
    }

    override fun read(timestampNanos: Long): TDigestResult = outerLock.guarded {
        compressLock.guarded {
            drainLocked()

            val total = totalWeightCell.load()
            val computed = DoubleArray(probabilities.size)
            if (means.isEmpty() || total <= 0.0) {
                // NaN per quantile rather than 0.0, matching DDSketchStat and AucStat. A
                // zero-filled array cannot be told apart from a digest that genuinely observed
                // zeros, so an untouched digest reported a p99 of 0.0 and read as healthy.
                // [TDigestResult.isEmpty] is the check for callers who would rather branch.
                computed.fill(Double.NaN)
                return@guarded TDigestResult(
                    probabilities,
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
                    q = means[idx] + frac * (means[idx + 1] - means[idx])
                }
                computed[pi] = q
            }

            TDigestResult(
                probabilities = probabilities,
                quantiles = computed,
                means = means.copyOf(),
                weights = weights.copyOf(),
                totalWeight = total,
                compression = compression,
            )
        }
    }
}

package com.eignex.kumulant.stat.change

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.sqrt

/** Snapshot from an [AdwinStat] adaptive-windowing change detector. */
@Serializable
@SerialName("AdwinResult")
data class AdwinResult(
    /** Confidence parameter for the Hoeffding-bound cut test. */
    val delta: Double,
    /** Number of observations currently inside the adaptive window. */
    val windowLength: Long,
    /** Mean of the observations currently inside the adaptive window. */
    val mean: Double,
    /** Population variance of the observations currently inside the adaptive window. */
    val variance: Double,
    /** Number of change points detected since [SeriesStat.reset]. */
    val changesDetected: Long,
    /** True when the most recent [SeriesStat.update] caused the window to shrink. */
    val alarm: Boolean,
) : Result

/**
 * ADWIN2 (Bifet & Gavaldà, 2007) adaptive-windowing change detector. Maintains an
 * exponential-histogram window of recent observations whose buckets grow as
 * `2^0, 2^1, 2^2, ...`; on every update the detector enumerates all bucket
 * boundaries and drops the older half whenever the mean difference exceeds the
 * Bernstein-flavoured Hoeffding bound
 *
 * ```
 * eps_cut = sqrt(2/m * sigma_W^2 * ln(2/delta')) + (2/(3m)) * ln(2/delta')
 * ```
 *
 * with `delta' = delta / ln(n)`, `m` the harmonic mean of the two side lengths, and
 * `sigma_W^2` the variance of the whole window. The window shrinks adaptively as
 * change points are detected; under stationary streams it grows toward the upper
 * memory bound, which is `maxBucketsPerSize * log2(window)` buckets.
 *
 * **Use cases:** drift detection in monitored signals where neither a target value
 * nor a fixed window is known up front (CUSUM and Page-Hinkley sit alongside).
 *
 * **Memory:** O(log n) buckets in the steady state; each bucket holds a
 * `count / sum / sumSquares` triplet.
 *
 * **Update:** O(log n) amortised; the inner change-test loop scans the bucket
 * adjacencies.
 *
 * **Concurrency:** Coupled exponential-histogram structure (category 3). The
 * internal lock keeps the multi-bucket update consistent under any [Concurrency]
 * level above [Concurrency.None].
 */
class AdwinStat(
    /** Confidence parameter for the cut test in `(0, 1)`. */
    val delta: Double = 0.002,
    /** Maximum number of buckets per power-of-two size class before merging upward. */
    val maxBucketsPerSize: Int = 5,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<AdwinResult> {

    init {
        require(delta > 0.0 && delta < 1.0) { "delta must be in (0, 1), got $delta" }
        require(maxBucketsPerSize >= 1) { "maxBucketsPerSize must be >= 1, got $maxBucketsPerSize" }
    }

    private class Bucket(var n: Long, var sum: Double, var sumSquares: Double)

    private val lock = concurrency.serializedLock()

    // rows[k] holds buckets of size 2^k. Front of each row is older than the back.
    // Across rows, the highest-k row holds the oldest buckets; row 0 holds the newest.
    private val rows: MutableList<ArrayDeque<Bucket>> = mutableListOf(ArrayDeque())
    private var totalN: Long = 0L
    private var totalSum: Double = 0.0
    private var totalSumSquares: Double = 0.0
    private var changesDetected: Long = 0L
    private var lastUpdateRaisedAlarm: Boolean = false

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Non-positive rather than merely inert: this recurrence has no inverse -
        // a bucket is appended, never taken back out - so the body below would run a
        // downdate forwards, as an ordinary observation. See Stat.
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            rows[0].addLast(Bucket(n = 1L, sum = value, sumSquares = value * value))
            totalN += 1L
            totalSum += value
            totalSumSquares += value * value
            compress()
            // Repeat the cut test until no significant change is found - matches Bifet's outer loop.
            var anyShrink = false
            while (detectAndShrink()) anyShrink = true
            lastUpdateRaisedAlarm = anyShrink
            if (anyShrink) changesDetected += 1L
        }
    }

    private fun compress() {
        var k = 0
        while (k < rows.size && rows[k].size > maxBucketsPerSize) {
            val row = rows[k]
            val b1 = row.removeFirst()
            val b2 = row.removeFirst()
            val merged = Bucket(n = b1.n + b2.n, sum = b1.sum + b2.sum, sumSquares = b1.sumSquares + b2.sumSquares)
            if (k + 1 >= rows.size) rows.add(ArrayDeque())
            // addLast, not addFirst: `merged` came from the two OLDEST buckets of row k, and row
            // k + 1 holds buckets older still, so within that row the merged one is the newest and
            // belongs at the back. Any other placement scrambles bucketsOldestToNewest(), so the cut
            // candidates stop being temporal prefixes and the window stops being a stream suffix.
            rows[k + 1].addLast(merged)
            k++
        }
    }

    // Scratch buffer for the oldest-to-newest bucket walk, reused across updates. The walk runs at
    // least once per observation, so a fresh list per call dominates update-path allocation.
    private val orderedScratch: MutableList<Bucket> = ArrayList()

    private fun bucketsOldestToNewest(): MutableList<Bucket> {
        orderedScratch.clear()
        // Indexed access rather than a for-in loop: kotlin.collections.ArrayDeque is a
        // MutableList, so this avoids allocating an iterator per row per call.
        for (k in rows.indices.reversed()) {
            val row = rows[k]
            for (i in 0 until row.size) orderedScratch.add(row[i])
        }
        return orderedScratch
    }

    private fun detectAndShrink(): Boolean {
        val ordered = bucketsOldestToNewest()
        if (ordered.size < 2 || totalN <= 1L) return false
        val deltaPrime = delta / ln(max(2.0, totalN.toDouble()))
        val n = totalN.toDouble()
        val mean = totalSum / n
        val variance = max(0.0, totalSumSquares / n - mean * mean)
        // Loop-invariant: depends only on deltaPrime, so hoisting it keeps a wide window from
        // paying one `ln` per candidate cut.
        val logTerm = ln(2.0 / deltaPrime)
        var n0 = 0L
        var sum0 = 0.0
        for (i in 0 until ordered.size - 1) {
            val b = ordered[i]
            n0 += b.n
            sum0 += b.sum
            val n1 = totalN - n0
            if (n0 == 0L || n1 == 0L) continue
            val mean0 = sum0 / n0.toDouble()
            val mean1 = (totalSum - sum0) / n1.toDouble()
            val m = 1.0 / (1.0 / n0.toDouble() + 1.0 / n1.toDouble())
            val epsCut = sqrt(2.0 / m * variance * logTerm) + (2.0 / (3.0 * m)) * logTerm
            if (abs(mean0 - mean1) > epsCut) {
                dropOldest(i + 1)
                return true
            }
        }
        return false
    }

    private fun dropOldest(bucketCount: Int) {
        var remaining = bucketCount
        for (k in rows.indices.reversed()) {
            val row = rows[k]
            while (remaining > 0 && row.isNotEmpty()) {
                val b = row.removeFirst()
                totalN -= b.n
                totalSum -= b.sum
                totalSumSquares -= b.sumSquares
                remaining -= 1
            }
            if (remaining == 0) break
        }
        while (rows.size > 1 && rows.last().isEmpty()) rows.removeAt(rows.size - 1)
    }

    override fun merge(values: AdwinResult, workspace: com.eignex.koblas.Workspace?) = lock.guarded {
        // No exact structural merge for the exponential histogram; carry over the change counter.
        changesDetected += values.changesDetected
    }

    override fun reset() = lock.guarded {
        rows.clear()
        rows.add(ArrayDeque())
        totalN = 0L
        totalSum = 0.0
        totalSumSquares = 0.0
        changesDetected = 0L
        lastUpdateRaisedAlarm = false
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        val n = totalN
        val mean = if (n > 0L) totalSum / n.toDouble() else 0.0
        val variance = if (n > 0L) max(0.0, totalSumSquares / n.toDouble() - mean * mean) else 0.0
        AdwinResult(
            delta = delta,
            windowLength = n,
            mean = mean,
            variance = variance,
            changesDetected = changesDetected,
            alarm = lastUpdateRaisedAlarm,
        )
    }

    override fun create(concurrency: Concurrency?) =
        AdwinStat(delta, maxBucketsPerSize, concurrency ?: this.concurrency)
}

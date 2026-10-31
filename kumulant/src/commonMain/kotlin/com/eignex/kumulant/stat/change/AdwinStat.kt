package com.eignex.kumulant.stat.change

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stat.StatDefaults
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
 * Merge adds the remote change count without reconstructing its adaptive window.
 * This is an approximate roll-up, not replay of the combined ordered observations.
 *
 * **Use cases:** drift detection in monitored signals where neither a target value
 * nor a fixed window is known up front (CUSUM and Page-Hinkley sit alongside).
 *
 * **Memory:** O(log n) buckets in the steady state; each bucket holds a
 * `count / mean / m2` triplet.
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
    val delta: Double = StatDefaults.ADWIN_DELTA,
    /** Maximum number of buckets per power-of-two size class before merging upward. */
    val maxBucketsPerSize: Int = StatDefaults.ADWIN_MAX_BUCKETS_PER_SIZE,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<AdwinResult> {

    init {
        require(delta > 0.0 && delta < 1.0) { "delta must be in (0, 1), got $delta" }
        require(maxBucketsPerSize >= 1) { "maxBucketsPerSize must be >= 1, got $maxBucketsPerSize" }
    }

    // Mean and centred second moment rather than raw power sums: `sumSquares / n - mean^2` cancels
    // catastrophically once |mean| >> sigma, the collapsed variance shrinks eps_cut to its bare
    // 2/(3m) term, and a stationary stream with a large offset raises a stream of false alarms.
    private class Bucket(val n: Long, val mean: Double, val m2: Double)

    private val lock = concurrency.serializedLock()

    // rows[k] holds buckets of size 2^k. Front of each row is older than the back.
    // Across rows, the highest-k row holds the oldest buckets; row 0 holds the newest.
    private val rows: MutableList<ArrayDeque<Bucket>> = mutableListOf(ArrayDeque())
    private var totalN: Long = 0L
    private var totalMean: Double = 0.0
    private var totalM2: Double = 0.0
    private var changesDetected: Long = 0L
    private var lastUpdateRaisedAlarm: Boolean = false

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Non-positive rather than merely inert: this recurrence has no inverse -
        // a bucket is appended, never taken back out - so the body below would run a
        // downdate forwards, as an ordinary observation. See Stat.
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            rows[0].addLast(Bucket(n = 1L, mean = value, m2 = 0.0))
            totalN += 1L
            val diff = value - totalMean
            totalMean += diff / totalN.toDouble()
            totalM2 += diff * (value - totalMean)
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
            val merged = combine(b1, b2)
            if (k + 1 >= rows.size) rows.add(ArrayDeque())
            // addLast, not addFirst: `merged` came from the two OLDEST buckets of row k, and row
            // k + 1 holds buckets older still, so within that row the merged one is the newest and
            // belongs at the back. Any other placement scrambles bucketsOldestToNewest(), so the cut
            // candidates stop being temporal prefixes and the window stops being a stream suffix.
            rows[k + 1].addLast(merged)
            k++
        }
    }

    private fun combine(a: Bucket, b: Bucket): Bucket {
        val n = a.n + b.n
        val gap = b.mean - a.mean
        val bShare = b.n.toDouble() / n.toDouble()
        return Bucket(
            n = n,
            mean = a.mean + gap * bShare,
            m2 = a.m2 + b.m2 + gap * gap * a.n.toDouble() * bShare,
        )
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
        val variance = max(0.0, totalM2 / totalN.toDouble())
        // Loop-invariant: depends only on deltaPrime, so hoisting it keeps a wide window from
        // paying one `ln` per candidate cut.
        val logTerm = ln(2.0 / deltaPrime)
        var n0 = 0L
        var mean0 = 0.0
        for (i in 0 until ordered.size - 1) {
            val b = ordered[i]
            n0 += b.n
            mean0 += (b.mean - mean0) * b.n.toDouble() / n0.toDouble()
            val n1 = totalN - n0
            if (n0 == 0L || n1 == 0L) continue
            // Suffix mean from the identity n * mean = n0 * mean0 + n1 * mean1, arranged to avoid
            // differencing two large sums.
            val mean1 = totalMean + (totalMean - mean0) * n0.toDouble() / n1.toDouble()
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
                row.removeFirst()
                remaining -= 1
            }
            if (remaining == 0) break
        }
        while (rows.size > 1 && rows.last().isEmpty()) rows.removeAt(rows.size - 1)
        // Refold the surviving buckets instead of subtracting the dropped ones: there are only
        // O(log n) of them, and a reverse Chan combine reintroduces the cancellation buckets avoid.
        totalN = 0L
        totalMean = 0.0
        totalM2 = 0.0
        for (k in rows.indices) {
            val row = rows[k]
            for (i in 0 until row.size) {
                val b = row[i]
                if (totalN == 0L) {
                    totalN = b.n
                    totalMean = b.mean
                    totalM2 = b.m2
                } else {
                    val folded = combine(Bucket(totalN, totalMean, totalM2), b)
                    totalN = folded.n
                    totalMean = folded.mean
                    totalM2 = folded.m2
                }
            }
        }
    }

    override fun merge(values: AdwinResult, timestampNanos: Long) = lock.guarded {
        // No exact structural merge for the exponential histogram; carry over the change counter.
        changesDetected += values.changesDetected
    }

    override fun reset() = lock.guarded {
        rows.clear()
        rows.add(ArrayDeque())
        totalN = 0L
        totalMean = 0.0
        totalM2 = 0.0
        changesDetected = 0L
        lastUpdateRaisedAlarm = false
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        val n = totalN
        val mean = if (n > 0L) totalMean else 0.0
        val variance = if (n > 0L) max(0.0, totalM2 / n.toDouble()) else 0.0
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

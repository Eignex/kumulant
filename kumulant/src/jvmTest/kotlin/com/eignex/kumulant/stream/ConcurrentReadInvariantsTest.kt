package com.eignex.kumulant.stream

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stat.anomaly.FeatureRange
import com.eignex.kumulant.stat.anomaly.HalfSpaceTreesStat
import com.eignex.kumulant.stat.calibration.IsotonicCalibratorStat
import com.eignex.kumulant.stat.calibration.ReliabilityStat
import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.quantile.HdrHistogramStat
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.score.AucStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.VarianceStat
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Concurrent read-path invariants across the catalogue.
 *
 * The pre-existing per-stat `*ConcurrencyTest` files in `commonTest` are single-threaded:
 * they replay a fixed sequence under each [Concurrency] level and compare against
 * [Concurrency.None]. That catches mode-wiring mistakes but structurally cannot catch a
 * missing lock (the sequential arithmetic is identical either way) or a torn read (there
 * is no concurrent reader). This suite covers exactly that gap.
 *
 * The shape is N writers plus one reader, and the assertions are *invariants* rather than
 * exact values: under a lock-free level a snapshot is allowed to be stale, but it is never
 * allowed to be impossible. An AUC above 1.0, a NaN quantile on a populated sketch, a
 * calibrated probability above 1.0, or an exception thrown out of `read()` are all bugs at
 * every level, and none of them are excused by the documented drift allowance.
 */
class ConcurrentReadInvariantsTest {

    private val levels = listOf(Concurrency.Strict, Concurrency.Relaxed, Concurrency.HighWrite)

    /**
     * Drive [stat] from [writers] threads while a further thread reads it, asserting
     * [invariants] on every snapshot.
     *
     * Failures are collected rather than thrown, because [runConcurrently] submits to an
     * executor and never inspects the returned futures, so anything thrown on a worker is
     * otherwise swallowed and the test passes green.
     */
    private fun <R : Result> assertReadInvariants(
        label: String,
        stat: Stat<R>,
        writers: Int = 6,
        iters: Int = 3_000,
        write: (threadId: Int, iter: Int) -> Unit,
        invariants: (R) -> Unit,
    ) {
        val failures: MutableList<Throwable> = Collections.synchronizedList(mutableListOf())
        runConcurrently(writers + 1, iters) { t, i ->
            try {
                if (t == 0) invariants(stat.read()) else write(t, i)
            } catch (e: Throwable) {
                failures.add(e)
            }
        }
        // Also check the settled state, which must satisfy the same invariants.
        try {
            invariants(stat.read())
        } catch (e: Throwable) {
            failures.add(e)
        }
        if (failures.isNotEmpty()) {
            val distinct = failures.map { "${it::class.simpleName}: ${it.message}" }.distinct()
            fail(
                "$label: ${failures.size} invariant violation(s) across ${distinct.size} kind(s):\n" +
                    distinct.joinToString(
                        "\n",
                    ).take(2000),
            )
        }
    }

    @Test
    fun `AucStat never reports an area outside the unit interval`() {
        for (level in levels) {
            val stat = AucStat(numBins = 32, concurrency = level)
            assertReadInvariants("AucStat[$level]", stat, write = { t, i ->
                val score = ((t * 31 + i * 7) % 100) / 100.0
                stat.update(score, if ((t + i) % 2 == 0) 1.0 else 0.0)
            }) { r ->
                assertTrue(r.totalPositives >= 0.0, "totalPositives=${r.totalPositives}")
                assertTrue(r.totalNegatives >= 0.0, "totalNegatives=${r.totalNegatives}")
                if (!r.auc.isNaN()) {
                    assertTrue(r.auc in 0.0..1.0, "auc=${r.auc}")
                }
                // The reported totals must account for exactly the binned weight.
                assertTrue(
                    abs(r.positives.sum() - r.totalPositives) < 1e-6,
                    "positives sum ${r.positives.sum()} vs total ${r.totalPositives}",
                )
                assertTrue(
                    abs(r.negatives.sum() - r.totalNegatives) < 1e-6,
                    "negatives sum ${r.negatives.sum()} vs total ${r.totalNegatives}",
                )
            }
        }
    }

    @Test
    fun `HdrHistogramStat read does not throw while buckets are being populated`() {
        for (level in levels) {
            // Spread values widely so fresh buckets keep appearing mid-read.
            val stat = HdrHistogramStat(concurrency = level)
            assertReadInvariants("HdrHistogramStat[$level]", stat, write = { t, i ->
                stat.update(1.0 + (t * 7919L + i * 104_729L) % 1_000_000L)
            }) { r ->
                assertTrue(
                    r.lowerBounds.size == r.upperBounds.size,
                    "lowers=${r.lowerBounds.size} uppers=${r.upperBounds.size}",
                )
                assertTrue(
                    r.lowerBounds.size == r.weights.size,
                    "lowers=${r.lowerBounds.size} weights=${r.weights.size}",
                )
                for (w in r.weights) assertTrue(w > 0.0, "non-positive weight $w in a populated-bucket list")
            }
        }
    }

    @Test
    fun `DDSketchStat never reports NaN quantiles on a populated sketch`() {
        for (level in levels) {
            val stat = DDSketchStat(relativeError = 0.01, concurrency = level)
            assertReadInvariants("DDSketchStat[$level]", stat, write = { t, i ->
                stat.update(1.0 + ((t * 13 + i * 17) % 10_000), weight = 0.1)
            }) { r ->
                if (r.totalWeights > 0.0) {
                    for ((j, q) in r.quantiles.withIndex()) {
                        assertTrue(
                            !q.isNaN(),
                            "quantile p=${r.probabilities[j]} was NaN with totalWeights=${r.totalWeights}",
                        )
                    }
                }
                var binned = r.zeroCount
                for (w in r.positiveBins.values) binned += w
                for (w in r.negativeBins.values) binned += w
                assertTrue(abs(binned - r.totalWeights) < 1e-6, "binned=$binned vs totalWeights=${r.totalWeights}")
            }
        }
    }

    @Test
    fun `ReliabilityStat never reports a rate outside the unit interval`() {
        for (level in levels) {
            val stat = ReliabilityStat(numBins = 16, concurrency = level)
            assertReadInvariants("ReliabilityStat[$level]", stat, write = { t, i ->
                stat.update(((t * 11 + i * 3) % 100) / 100.0, if ((t + i) % 3 == 0) 1.0 else 0.0)
            }) { r ->
                for (v in r.outcomeRate) if (!v.isNaN()) assertTrue(v in 0.0..1.0, "outcomeRate=$v")
                for (v in r.meanProbability) if (!v.isNaN()) assertTrue(v in 0.0..1.0, "meanProbability=$v")
                for (i in 0 until r.numBins) {
                    assertTrue(
                        r.sumOutcome[i] <= r.totalWeights[i] + 1e-9,
                        "sumOutcome ${r.sumOutcome[i]} > totalWeights ${r.totalWeights[i]}",
                    )
                }
            }
        }
    }

    @Test
    fun `IsotonicCalibratorStat never emits a probability outside the unit interval`() {
        for (level in levels) {
            val stat = IsotonicCalibratorStat(numBins = 16, concurrency = level)
            assertReadInvariants("IsotonicCalibratorStat[$level]", stat, write = { t, i ->
                stat.update(((t * 11 + i * 3) % 100) / 100.0, if ((t + i) % 3 == 0) 1.0 else 0.0)
            }) { r ->
                for (p in listOf(0.0, 0.25, 0.5, 0.75, 1.0)) {
                    val c = r.calibrate(p)
                    assertTrue(!c.isNaN() && c in 0.0..1.0, "calibrate($p)=$c")
                }
            }
        }
    }

    @Test
    fun `HalfSpaceTreesStat accumulates every observation and scores finitely`() {
        for (level in levels) {
            val ranges = List(3) { FeatureRange(0.0, 1.0) }
            val stat = HalfSpaceTreesStat(
                featureSize = 3,
                featureRanges = ranges,
                numTrees = 4,
                height = 3,
                windowSize = 500,
                concurrency = level,
            )
            val writers = 6
            val iters = 3_000
            assertReadInvariants("HalfSpaceTreesStat[$level]", stat, writers = writers, iters = iters, write = { t, i ->
                val v = DenseVector.of(
                    doubleArrayOf(
                        ((t * 7 + i) % 100) / 100.0,
                        ((t * 13 + i * 3) % 100) / 100.0,
                        ((t * 29 + i * 5) % 100) / 100.0,
                    ),
                )
                stat.update(v)
            }) { r ->
                assertTrue(r.totalWeights >= 0.0, "totalWeights=${r.totalWeights}")
                for (m in r.referenceMass) assertTrue(m >= 0.0 && m.isFinite(), "referenceMass entry $m")
                val s = r.score(DenseVector.of(doubleArrayOf(0.5, 0.5, 0.5)))
                assertTrue(s.isFinite() && s >= 0.0, "score=$s")
            }
            val fed = (writers * iters).toDouble()
            val total = stat.read().totalWeights
            assertTrue(total <= fed + 1e-6, "$level: totalWeights $total exceeded the $fed observations fed")
            // Strict and HighWrite promise exactness for a purely additive accumulation, so
            // every observation must be accounted for. This is the assertion that catches a
            // missing lock or a non-atomic cell: lost increments leave the total short while
            // still satisfying the upper bound above, so `<=` alone passes on broken code.
            // Relaxed is excluded because its contract permits drift.
            if (level != Concurrency.Relaxed) {
                assertTrue(
                    abs(total - fed) < 1e-6,
                    "$level promises exact additive accumulation but totalWeights was $total for $fed observations, " +
                        "so ${fed - total} increments were lost",
                )
            }
        }
    }

    @Test
    fun `Welford summary stats stay finite and self-consistent under a concurrent reader`() {
        for (level in levels) {
            val mean = MeanStat(level)
            assertReadInvariants("MeanStat[$level]", mean, write = { t, i ->
                mean.update(1.0 + ((t * 3 + i) % 50))
            }) { r ->
                assertTrue(r.totalWeights >= 0.0, "totalWeights=${r.totalWeights}")
                assertTrue(r.mean.isFinite(), "mean=${r.mean}")
                if (r.totalWeights > 0.0) assertTrue(r.mean in 0.0..60.0, "mean ${r.mean} outside the fed range")
            }

            val variance = VarianceStat(level)
            assertReadInvariants("VarianceStat[$level]", variance, write = { t, i ->
                variance.update(1.0 + ((t * 3 + i) % 50))
            }) { r ->
                assertTrue(r.variance.isFinite(), "variance=${r.variance}")
                assertTrue(r.variance >= -1e-9, "negative variance ${r.variance}")
            }

            val moments = MomentsStat(level)
            assertReadInvariants("MomentsStat[$level]", moments, write = { t, i ->
                moments.update(1.0 + ((t * 3 + i) % 50))
            }) { r ->
                assertTrue(r.m2 >= -1e-9, "negative m2 ${r.m2}")
                assertTrue(r.m4 >= -1e-9, "negative m4 ${r.m4}")
                assertTrue(
                    r.mean.isFinite() && r.m2.isFinite() && r.m3.isFinite() && r.m4.isFinite(),
                    "non-finite moment in $r",
                )
            }
        }
    }

    @Test
    fun `TDigestStat quantiles stay ordered and finite under a concurrent reader`() {
        for (level in levels) {
            val stat = TDigestStat(concurrency = level)
            assertReadInvariants("TDigestStat[$level]", stat, write = { t, i ->
                stat.update(1.0 + ((t * 13 + i * 17) % 10_000))
            }) { r ->
                if (r.totalWeight > 0.0) {
                    var prev = Double.NEGATIVE_INFINITY
                    for ((j, q) in r.quantiles.withIndex()) {
                        assertTrue(q.isFinite(), "quantile p=${r.probabilities[j]} was $q")
                        assertTrue(
                            q >= prev - 1e-6,
                            "quantiles not monotone at p=${r.probabilities[j]}: $q after $prev",
                        )
                        prev = q
                    }
                }
            }
        }
    }

    private fun abs(x: Double) = if (x < 0.0) -x else x
}

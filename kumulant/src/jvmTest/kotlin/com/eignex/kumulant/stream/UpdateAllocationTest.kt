package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.change.AdwinStat
import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.quantile.HdrHistogramStat
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Allocation on the update path, measured rather than assumed.
 *
 * kotlinx-benchmark 0.4.13 does not expose JMH's `gc` profiler - its `advanced` options are
 * limited to fork and bridge settings - so per-operation allocation is invisible to the
 * benchmark suite. This measures it directly through the HotSpot thread allocation counter,
 * which is exact for a single-threaded loop and cheap enough to run as an ordinary test.
 *
 * Thresholds are ceilings meant to catch a regression, not exact figures. Measured values at
 * the time of writing are in each assertion's message.
 */
class UpdateAllocationTest {

    private val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean

    /**
     * Loop body taking a primitive `Int`.
     *
     * A Kotlin `(Int) -> Unit` compiles to `Function1<Integer, Unit>`, so every call boxes the
     * index and the harness allocates 16 B per iteration, swamping what is being measured. A
     * `fun interface` over a primitive parameter compiles to `(I)V` and allocates nothing.
     */
    private fun interface IntBody {
        fun run(i: Int)
    }

    /**
     * Best-of-five bytes per iteration.
     *
     * Warmup matters more than it looks: at 20k iterations the JIT has not settled and even a
     * genuinely allocation-free stat reports several bytes per op. Taking the minimum of
     * several passes after a 50k warmup removes that noise.
     */
    private fun bytesPerOp(iterations: Int, body: IntBody): Double {
        repeat(50_000) { body.run(it) }
        val id = Thread.currentThread().threadId()
        var best = Double.MAX_VALUE
        repeat(5) {
            val before = bean.getThreadAllocatedBytes(id)
            for (i in 0 until iterations) body.run(i)
            val after = bean.getThreadAllocatedBytes(id)
            val per = (after - before).toDouble() / iterations
            if (per < best) best = per
        }
        return best
    }

    private fun assertUpdateAllocation(name: String, limit: Double, stat: SeriesStat<*>, span: Int = 97) {
        val perOp = bytesPerOp(200_000) { i -> stat.update(1.0 + (i % span), 0L, 1.0) }
        assertTrue(perOp <= limit, "$name allocated $perOp B/update, expected at most $limit")
    }

    @Test
    fun `summary stats allocate nothing per update`() {
        assertUpdateAllocation("SumStat", 0.0, SumStat(Concurrency.None))
        assertUpdateAllocation("MeanStat", 0.0, MeanStat(Concurrency.None))
        assertUpdateAllocation("VarianceStat", 0.0, VarianceStat(Concurrency.None))
        assertUpdateAllocation("MomentsStat", 0.0, MomentsStat(Concurrency.None))
    }

    /**
     * Taking a lock must not allocate.
     *
     * `update` bodies used to be written `lock.withLock { ... }`. `withLock` is a non-inline
     * interface method taking a function, so each call constructed a capturing lambda. The JIT
     * scalar-replaced it only while the call site stayed monomorphic: measured alone this test
     * reported 0 B/update under [Concurrency.Strict], but in a full-suite run, where both
     * `NoopMutex` and `PlatformMutex` have been used, escape analysis gave up and it was
     * 32 B/update. A production process looks like the second case.
     *
     * The bodies now use the inlining `guarded`, which expands to a direct
     * `enter`/`try`/`finally`/`exit` and constructs nothing, so these are exact zeroes in both
     * contexts rather than only in isolation. Kotlin platforms without escape analysis, which
     * this counter cannot measure, benefit unconditionally.
     */
    @Test
    fun `taking a lock does not allocate on the update path`() {
        assertUpdateAllocation("MeanStat[None]", 0.0, MeanStat(Concurrency.None))
        assertUpdateAllocation("MeanStat[Strict]", 0.0, MeanStat(Concurrency.Strict))
        assertUpdateAllocation("MomentsStat[Strict]", 0.0, MomentsStat(Concurrency.Strict))
    }

    @Test
    fun `sketches allocate nothing per update`() {
        assertUpdateAllocation("DDSketchStat", 0.0, DDSketchStat(concurrency = Concurrency.None), span = 9973)
        assertUpdateAllocation("HdrHistogramStat", 0.0, HdrHistogramStat(concurrency = Concurrency.None))
    }

    /**
     * Ceilings for the two stats that allocate by design, as tripwires against a large
     * regression rather than as precise figures.
     *
     * These have headroom but are no longer loose. They used to need a wide margin because the
     * figures moved with the JIT state left behind by preceding tests - TDigest measured ~55
     * B/op alone and ~86 B/op in a full-suite run. That gap was the lock lambda, and with
     * `guarded` inlining it away both stats now report the same number under every
     * [Concurrency] level in either context, so the remaining allocation is purely structural:
     * one Bucket per observation for Adwin, the merged centroid arrays per compression epoch
     * for TDigest.
     */
    @Test
    fun `the amortised allocators stay under their ceiling`() {
        // AdwinStat allocates one Bucket per observation by design, and its bucket-walk scratch
        // buffer grows with the window. Measured 277 B/op before the scratch buffer was made
        // reusable, 75 B/op after, and the same under Strict now that the lock does not allocate.
        assertUpdateAllocation("AdwinStat", 110.0, AdwinStat(concurrency = Concurrency.None))
        // TDigestStat allocates the merged centroid arrays once per compression epoch, amortised
        // over the buffer. Measured 82 B/op before the index sort stopped boxing, 54 B/op after.
        assertUpdateAllocation("TDigestStat", 80.0, TDigestStat(concurrency = Concurrency.None), span = 9973)
    }
}

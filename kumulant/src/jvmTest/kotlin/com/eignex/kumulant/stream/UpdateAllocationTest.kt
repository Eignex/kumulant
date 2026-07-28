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
     * The cost of the lock, pinned as it actually behaves.
     *
     * `update` bodies are written `lock.withLock { ... }`, and `withLock` is a non-inline
     * interface method, so each call constructs a capturing lambda. Whether that lambda costs
     * anything depends on the call site:
     *
     * - Under [Concurrency.None] the lock is `NoopMutex` and the measured cost is zero.
     * - Under [Concurrency.Strict] it is 32 B/update **once more than one `Mutex`
     *   implementation is live in the process**. Measured in isolation this test reports zero,
     *   because the call site stays monomorphic and the JIT scalar-replaces the lambda; measured
     *   in a full-suite run, where both `NoopMutex` and `PlatformMutex` have been used, escape
     *   analysis gives up and the allocation is real. A real application looks like the latter.
     *
     * Removing it needs an `enter`/`exit` split so no lambda is constructed at all, which
     * touches every platform's `PlatformMutex` plus every `update` body. Not done here; this
     * pins the current cost so the eventual fix has a baseline and a regression cannot creep in.
     * Kotlin/Native has no comparable escape analysis and cannot be measured with this counter,
     * so the cost there is likely unconditional.
     */
    @Test
    fun `the lock allocation on the update path stays at its known cost`() {
        assertUpdateAllocation("MeanStat[None]", 0.0, MeanStat(Concurrency.None))
        assertUpdateAllocation("MeanStat[Strict]", 48.0, MeanStat(Concurrency.Strict))
        assertUpdateAllocation("MomentsStat[Strict]", 48.0, MomentsStat(Concurrency.Strict))
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
     * These are deliberately loose. Unlike the zero-allocation assertions above, which are
     * exact because zero is zero, an amortised figure moves with the JIT state left behind by
     * whatever ran before it: TDigest measures ~55 B/op when this class runs alone and ~86
     * B/op in a full-suite run. The ceilings sit above the noisier of the two.
     */
    @Test
    fun `the amortised allocators stay under their ceiling`() {
        // AdwinStat allocates one Bucket per observation by design, and its bucket-walk scratch
        // buffer grows with the window. Measured 277 B/op before the scratch buffer was made
        // reusable, 75 B/op after.
        assertUpdateAllocation("AdwinStat", 160.0, AdwinStat(concurrency = Concurrency.None))
        // TDigestStat allocates the merged centroid arrays once per compression epoch, amortised
        // over the buffer. Measured 82 B/op before the index sort stopped boxing, 55 B/op after.
        assertUpdateAllocation("TDigestStat", 130.0, TDigestStat(concurrency = Concurrency.None), span = 9973)
    }
}

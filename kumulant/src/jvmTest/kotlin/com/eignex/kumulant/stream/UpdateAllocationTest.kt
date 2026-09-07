package com.eignex.kumulant.stream

import com.eignex.kumulant.ALLOCATION_PASS_CALLS
import com.eignex.kumulant.assertAllocatesAtMost
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
import kotlin.test.Test

// Thresholds are ceilings meant to catch a regression, not exact figures.
class UpdateAllocationTest {

    private fun assertUpdateAllocation(
        name: String,
        limit: Double,
        stat: SeriesStat<*>,
        span: Int = 97,
        callsPerPass: Int = ALLOCATION_PASS_CALLS,
    ) = assertAllocatesAtMost(limit, "$name update", callsPerPass) { i -> stat.update(1.0 + (i % span), 0L, 1.0) }

    @Test
    fun `summary stats allocate nothing per update`() {
        assertUpdateAllocation("SumStat", 0.0, SumStat(Concurrency.None))
        assertUpdateAllocation("MeanStat", 0.0, MeanStat(Concurrency.None))
        assertUpdateAllocation("VarianceStat", 0.0, VarianceStat(Concurrency.None))
        assertUpdateAllocation("MomentsStat", 0.0, MomentsStat(Concurrency.None))
    }

    @Test
    fun `taking a lock does not allocate on the update path`() {
        // `update` bodies use the inlining `guarded`, which expands to a direct
        // `enter`/`try`/`finally`/`exit` and constructs nothing. A `lock.withLock { ... }` would
        // construct a capturing lambda that the JIT scalar-replaces only while the call site stays
        // monomorphic, so it measures 0 B/update in isolation and 32 B/update in a full-suite run,
        // where both `NoopMutex` and `PlatformMutex` have been seen. A production process looks like
        // the second case.
        assertUpdateAllocation("MeanStat[None]", 0.0, MeanStat(Concurrency.None))
        assertUpdateAllocation("MeanStat[Strict]", 0.0, MeanStat(Concurrency.Strict))
        assertUpdateAllocation("MomentsStat[Strict]", 0.0, MomentsStat(Concurrency.Strict))
    }

    @Test
    fun `sketches allocate nothing per update`() {
        assertUpdateAllocation("DDSketchStat", 0.0, DDSketchStat(concurrency = Concurrency.None), span = 9973)
        assertUpdateAllocation("HdrHistogramStat", 0.0, HdrHistogramStat(concurrency = Concurrency.None))
    }

    @Test
    fun `the amortised allocators stay under their ceiling`() {
        // Ceilings for the two stats that allocate by design, as tripwires against a large regression
        // rather than as precise figures. Both allocate the same amount under every [Concurrency]
        // level, so what is left is structural. Their allocation lands in bursts, so a pass has to
        // span many of them for the per-update figure to mean anything.
        //
        // AdwinStat allocates one Bucket per observation, and its bucket-walk scratch buffer grows
        // with the window. Measured 75 B/op.
        assertUpdateAllocation("AdwinStat", 110.0, AdwinStat(concurrency = Concurrency.None), callsPerPass = 200_000)
        // TDigestStat allocates the merged centroid arrays once per compression epoch, amortised
        // over the buffer. Measured 54 B/op.
        assertUpdateAllocation(
            "TDigestStat",
            80.0,
            TDigestStat(concurrency = Concurrency.None),
            span = 9973,
            callsPerPass = 200_000,
        )
    }
}

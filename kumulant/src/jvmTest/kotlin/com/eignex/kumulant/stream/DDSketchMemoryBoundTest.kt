package com.eignex.kumulant.stream

import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class DDSketchMemoryBoundTest {

    private val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean

    private fun heapGrowth(body: () -> Unit): Long {
        val id = Thread.currentThread().threadId()
        val before = bean.getThreadAllocatedBytes(id)
        body()
        return bean.getThreadAllocatedBytes(id) - before
    }

    @Test
    fun `extreme values do not blow up the bin array`() {
        val limits = mapOf(0.01 to 512L * 1024, 0.001 to 4L * 1024 * 1024)
        for ((relativeError, limit) in limits) {
            val sketch = DDSketchStat(relativeError = relativeError)
            sketch.update(1.0)
            val grown = heapGrowth {
                sketch.update(Double.MIN_VALUE)
                sketch.update(Double.MAX_VALUE)
            }
            assertTrue(
                grown <= limit,
                "relativeError=$relativeError grew the heap by ${grown / 1024} KiB on two " +
                    "extreme observations, expected at most ${limit / 1024} KiB",
            )
        }
    }

    @Test
    fun `out-of-range values still count toward the rank`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        sketch.update(Double.MIN_VALUE)
        sketch.update(1.0)
        sketch.update(Double.MAX_VALUE)
        val r = sketch.read()
        assertTrue(r.totalWeights == 3.0, "expected all three observations counted, got ${r.totalWeights}")
        assertTrue(r.quantiles[0] > 0.0 && r.quantiles[0].isFinite(), "p50 was ${r.quantiles[0]}")
    }
}

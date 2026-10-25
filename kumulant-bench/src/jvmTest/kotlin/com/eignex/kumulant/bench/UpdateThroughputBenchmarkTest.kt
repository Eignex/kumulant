package com.eignex.kumulant.bench

import com.eignex.kumulant.stat.rate.RateResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class UpdateThroughputBenchmarkTest {
    @Test
    fun `repeated timestamped batches advance time and retain every update`() {
        val benchmark = UpdateThroughputBenchmark().apply { name = "RateStat"; setup() }
        benchmark.update()
        val first = assertIs<RateResult>(benchmark.snapshot())

        benchmark.update()
        val second = assertIs<RateResult>(benchmark.snapshot())

        assertEquals(first.totalValue * 2, second.totalValue, 1e-9)
        assertEquals(STAT_BENCHMARK_BATCH_SIZE * TIME_PROGRESSING_STRIDE_NANOS, second.timestampNanos - first.timestampNanos)
    }
}

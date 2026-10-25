package com.eignex.kumulant.bench

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CaptureConcurrencyTest {
    @Test
    fun `worker failures are propagated without waiting for another blocked worker`() {
        val blocked = CountDownLatch(1)

        val failure = assertFailsWith<IllegalArgumentException> {
            runCaptureWorkers(listOf({ blocked.await() }, { throw IllegalArgumentException("worker failed") }))
        }

        assertEquals("worker failed", failure.message)
    }

    @Test
    fun `reader failures stop a concurrent workload`() {
        val failure = assertFailsWith<IllegalStateException> {
            runCaptureWorkers(listOf({ CountDownLatch(1).await() }), reader = { error("reader failed") })
        }

        assertEquals("reader failed", failure.message)
    }

    @Test
    fun `a stalled worker is interrupted when the workload times out`() {
        assertFailsWith<IllegalStateException> {
            runCaptureWorkers(listOf({ CountDownLatch(1).await() }), timeoutMillis = 20L)
        }
    }

    @Test
    fun `every scheduled update completes before the runner returns`() {
        val count = AtomicInteger()

        runCaptureWorkers(List(4) { { repeat(100) { count.incrementAndGet() } } })

        assertEquals(400, count.get())
    }

    @Test
    fun `shared stat updates preserve observations while snapshots are read`() {
        val case = captureCases("smoke").single { it.id == "concurrency/variance/update/Strict/4" }

        val validation = concurrencyCaptureQuality(case.copy(workloadParameters = case.workloadParameters + ("observations" to "64")))

        assertTrue(validation.passed, validation.metrics.toString())
        assertEquals(0.0, validation.metrics.single { it.name == "observationCountError" }.value)
        assertTrue(validation.metrics.single { it.name == "snapshotReads" }.value >= 1.0)
    }

    @Test
    fun `concurrent merges preserve all source observations`() {
        val case = captureCases("smoke").single { it.id == "concurrency/sum/merge/Strict/4" }

        val validation = concurrencyCaptureQuality(case.copy(workloadParameters = case.workloadParameters + ("observations" to "64")))

        assertTrue(validation.passed, validation.metrics.toString())
        assertEquals(0.0, validation.metrics.single { it.name == "observationCountError" }.value)
    }

    @Test
    fun `concurrency selection excludes serial workloads`() {
        val cases = selectCaptureCases("smoke", null, "*", "concurrency")

        assertTrue(cases.all { it.suite == "concurrency" })
        assertTrue(selectCaptureCases("smoke", null, "*", "performance").none { it.suite == "concurrency" })
    }

    @Test
    fun `unsynchronized stats cannot be shared between writers`() {
        val case = captureCases("smoke").single { it.id == "concurrency/sum/update/Strict/4" }

        assertFailsWith<IllegalArgumentException> {
            concurrencyCaptureQuality(case.copy(workloadParameters = case.workloadParameters + ("concurrency" to "None")))
        }
    }

    @Test
    fun `aggregate batch throughput converts to updates per second`() {
        val case = captureCases("smoke").single { it.id == "concurrency/sum/update/Strict/4" }
        val report = """[{
            "benchmark":"com.eignex.kumulant.bench.UpdateThroughputBenchmark.update",
            "mode":"thrpt", "params":{"name":"SumStat", "concurrency":"Strict"},
            "primaryMetric":{"score":1000.0,"scoreError":100.0,"scoreUnit":"ops/s"}
        }]"""

        val metrics = jmhMetrics(report, case)

        assertEquals(4096000.0, metrics.single { it.name == "throughput" }.value)
        assertEquals(1e9 / 4096000, metrics.single { it.name == "time" }.value)
        assertEquals(409600.0, metrics.single { it.name == "throughputError" }.value)
    }

    @Test
    fun `scaling compares the same stat operation backend and concurrency level`() {
        val single = captureCases("smoke").single { it.id == "concurrency/sum/update/Strict/1" }
        val multi = captureCases("smoke").single { it.id == "concurrency/sum/update/Strict/4" }
        val relaxed = captureCases("smoke").single { it.id == "concurrency/sum/update/Relaxed/1" }
        val results = listOf(
            CaptureResult(single, "jvm-simd", "passed", metrics = listOf(CaptureMetric("throughput", 1000.0, "operations/s"))),
            CaptureResult(relaxed, "jvm-scalar", "passed", metrics = listOf(CaptureMetric("throughput", 1000.0, "operations/s"))),
            CaptureResult(single, "jvm-scalar", "passed", metrics = listOf(CaptureMetric("throughput", 100.0, "operations/s"))),
            CaptureResult(multi, "jvm-scalar", "passed", metrics = listOf(CaptureMetric("throughput", 200.0, "operations/s"))),
        )

        val scaled = captureScalingResults(results).last()

        assertEquals(2.0, scaled.metrics.single { it.name == "threadScaling" }.value)
        assertEquals(0.5, scaled.metrics.single { it.name == "parallelEfficiency" }.value)
    }
}

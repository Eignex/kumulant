package com.eignex.kumulant.bench

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CaptureReportTest {
    @Test
    fun `batch timings and allocation normalize to individual updates`() {
        val case = captureCases("smoke").first()
        val report = """[{
            "benchmark":"com.eignex.kumulant.bench.UpdateThroughputBenchmark.update",
            "mode":"avgt", "params":{"name":"SumStat", "concurrency":"None"},
            "primaryMetric":{"score":8192.0,"scoreError":4096.0,"scoreUnit":"ns/op"},
            "secondaryMetrics":{"gc.alloc.rate.norm":{"score":2048.0,"scoreUnit":"B/op"}}
        }]"""

        val metrics = jmhMetrics(report, case)

        assertEquals(CaptureMetric("time", 2.0, "ns/operation"), metrics[0])
        assertEquals(CaptureMetric("timeError", 1.0, "ns/operation"), metrics[1])
        assertEquals(CaptureMetric("allocation", 0.5, "bytes/operation"), metrics[2])
    }

    @Test
    fun `timings for another case cannot be accepted`() {
        val case = captureCases("smoke").first()
        val report = """[{
            "benchmark":"com.eignex.kumulant.bench.UpdateThroughputBenchmark.update",
            "mode":"avgt", "params":{"name":"VarianceStat", "concurrency":"None"},
            "primaryMetric":{"score":100.0,"scoreUnit":"ns/op"}
        }]"""

        assertFailsWith<IllegalArgumentException> { jmhMetrics(report, case) }
    }

    @Test
    fun `throughput and nonfinite scores cannot be published as timings`() {
        val case = captureCases("smoke").first()
        for ((mode, score, unit) in listOf(
            Triple("thrpt", "100.0", "ops/ns"),
            Triple("avgt", "\"NaN\"", "ns/op"),
            Triple("avgt", "0.0", "ns/op"),
        )) {
            val report = """[{
                "benchmark":"com.eignex.kumulant.bench.UpdateThroughputBenchmark.update",
                "mode":"$mode", "params":{"name":"SumStat", "concurrency":"None"},
                "primaryMetric":{"score":$score,"scoreUnit":"$unit"}
            }]"""

            assertFailsWith<IllegalArgumentException> { jmhMetrics(report, case) }
        }
    }

    @Test
    fun `nonfinite and excessive validation errors fail the capture`() {
        for (value in listOf(Double.NaN, Double.POSITIVE_INFINITY, 0.1)) {
            val validation = CaptureValidation("scalar", "fingerprint", listOf(CaptureMetric("error", value, "ratio", 1e-10)))

            assertFalse(validation.passed)
        }
        assertFalse(CaptureValidation("scalar", "fingerprint", emptyList()).passed)
    }

    @Test
    fun `partial reports preserve a failed case and pending work`() {
        val directory = Files.createTempDirectory("capture-report")
        val cases = captureCases("smoke")
        val results = listOf(
            CaptureResult(cases[0], "jvm-scalar", "failed", reason = "quality failure"),
            CaptureResult(cases[1], "jvm-scalar", "pending"),
        )
        try {
            writeCaptureResults(directory, results, "incomplete")

            val decoded = captureJson.decodeFromString<CaptureResults>(Files.readString(directory.resolve("results.json")))
            assertEquals("incomplete", decoded.status)
            assertEquals(results, decoded.results)
            assertTrue(Files.readString(directory.resolve("summary.md")).contains("quality failure"))
        } finally {
            Files.deleteIfExists(directory.resolve("results.json"))
            Files.deleteIfExists(directory.resolve("summary.md"))
            Files.delete(directory)
        }
    }
}

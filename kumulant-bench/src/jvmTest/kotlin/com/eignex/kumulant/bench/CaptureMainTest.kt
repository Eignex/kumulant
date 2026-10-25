package com.eignex.kumulant.bench

import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CaptureMainTest {
    @Test
    fun `larger presets retain representative cases from smaller presets`() {
        val smoke = captureCases("smoke").toSet()
        val standard = captureCases("standard").toSet()
        val extended = captureCases("extended").toSet()

        assertTrue(standard.containsAll(smoke))
        assertTrue(extended.containsAll(standard))
    }

    @Test
    fun `case filters select stable IDs with literal punctuation`() {
        val selected = selectCaptureCases("standard", "cholesky", "cholesky/factor/rbf/*")

        assertEquals(listOf("cholesky/factor/rbf/128"), selected.map { it.id })
        assertFailsWith<IllegalArgumentException> { selectCaptureCases("smoke", null, "stats/.*/update") }
    }

    @Test
    fun `empty and unknown selections are rejected`() {
        for (args in listOf(
            Triple("smoke", "missing", "*"),
            Triple("missing", null, "*"),
            Triple("smoke", "stats", "cholesky/*"),
        )) {
            assertFailsWith<IllegalArgumentException> { selectCaptureCases(args.first, args.second, args.third) }
        }
    }

    @Test
    fun `invalid capture options are rejected before measuring`() {
        for (args in listOf(
            arrayOf("--targets", "native"),
            arrayOf("--targets", "jvm-scalar,jvm-scalar"),
            arrayOf("--preset"),
            arrayOf("--case", "--list"),
            arrayOf("--mode", "missing"),
            arrayOf("--unknown", "value"),
            arrayOf("--mode", "quality", "--allocation"),
            arrayOf("--output", ""),
            arrayOf("--list", "--list"),
        )) {
            assertFailsWith<IllegalArgumentException> { captureOptions(args) }
        }
    }

    @Test
    fun `existing evidence cannot be overwritten`() {
        val directory = Files.createTempDirectory("capture-output")
        try {
            Files.writeString(directory.resolve("evidence.json"), "retained")

            assertFailsWith<IllegalArgumentException> { prepareCaptureOutput(directory) }

            assertEquals("retained", Files.readString(directory.resolve("evidence.json")))
        } finally {
            Files.delete(directory.resolve("evidence.json"))
            Files.delete(directory)
        }
    }

    @Test
    fun `timing restricts JMH to one case and its complete parameters`() {
        val case = captureCases("smoke").first()

        val command = captureJmhArguments(case, captureTiming("smoke"), Path.of("raw.json"), allocation = true)

        assertEquals("org.openjdk.jmh.Main", command.first())
        assertTrue(command[1].toRegex().matches("com.eignex.kumulant.bench.UpdateThroughputBenchmark.update"))
        assertEquals("avgt", command[command.indexOf("-bm") + 1])
        assertEquals(listOf("name=SumStat", "concurrency=None"), command.windowed(2).filter { it[0] == "-p" }.map { it[1] })
        assertEquals("gc", command[command.indexOf("-prof") + 1])
    }
}

package com.eignex.kumulant.bench

import com.eignex.lab.bench.BenchContext
import com.eignex.lab.bench.BenchStatus
import java.nio.file.Files
import com.eignex.lab.bench.BenchInput
import com.eignex.lab.bench.BenchRequest
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import org.openjdk.jmh.runner.options.CommandLineOptions
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class LabAdapterTest {
    private fun plan(vararg parameters: Pair<String, String>) = KumulantLabSuite.plan(BenchRequest(mapOf(*parameters)))

    @Test fun `JMH arguments retain repeated parameters and size the CPU reservation`() {
        val case = plan("backends" to "scalar", "argv" to "[\"-t\",\"4\",\"-p\",\"name=SumStat\",\"-p\",\"concurrency=Strict\"]").single()
        val argv = com.eignex.lab.bench.BenchProtocol.json.parseToJsonElement(case.parameters.getValue("argv")).jsonArray.map { it.jsonPrimitive.content }
        assertEquals(listOf("-t", "4", "-p", "name=SumStat", "-p", "concurrency=Strict"), argv.take(6))
        assertEquals(4, case.resources.cpus)
        assertEquals(2, CommandLineOptions(*argv.toTypedArray()).forkCount.get())
        assertEquals(listOf("SumStat"), CommandLineOptions(*argv.toTypedArray()).getParameter("name").get().toList())
    }

    @Test fun `prepared native plans pin both executables and the Linux platform`() {
        val case = plan("backends" to "native").single()
        assertEquals(2, case.inputs.size)
        assertEquals(setOf("platform:linux", "arch:x86_64"), case.resources.requiredTags)
        assertEquals("2", case.parameters["repetitions"])
        assertTrue(case.inputs.all { it.path in case.parameters.values })
    }

    @Test fun `portable native executable selections are declared inputs`() {
        val executable = "kumulant-bench/build/bin/linuxX64/custom/bench.kexe"
        val case = plan("backends" to "native", "native-executable" to executable).single()
        assertTrue(BenchInput(executable, role = "executable") in case.inputs)
    }

    @Test fun `concurrency reserves all writers and the snapshot reader`() {
        assertEquals(5, plan("backends" to "simd", "mode" to "concurrency").single().resources.cpus)
    }

    @Test fun `unsupported and ignored selections are rejected during planning`() {
        val invalid = listOf(
            mapOf("backends" to "native", "mode" to "concurrency"),
            mapOf("backends" to "scalar,scalar"), mapOf("backends" to ""),
            mapOf("backends" to "gpu"), mapOf("smoke" to "yes"), mapOf("other" to "1"),
            mapOf("repetitions" to "0"), mapOf("timeout-seconds" to "0"), mapOf("memory-mib" to "1024"),
            mapOf("heap-mib" to "1024"), mapOf("jvm-gc" to "invalid"), mapOf("stress-order" to "16"),
            mapOf("backends" to "native", "jfr" to "true"), mapOf("backends" to "native", "heap-mib" to "128"),
            mapOf("mode" to "quality", "argv" to "[\"-t\",\"4\"]"),
            mapOf("backends" to "scalar", "native-executable" to "kumulant-bench/build/linuxX64/bench.kexe"),
        )
        invalid.forEach { assertFailsWith<IllegalArgumentException>(it.toString()) { KumulantLabSuite.plan(BenchRequest(it)) } }
    }

    @Test fun `prepared JVM requests cannot override backend memory or artifact ownership`() {
        for (argv in listOf(
            "[\"-jvmArgs\",\"--add-modules=jdk.incubator.vector\"]", "[\"-rff\",\"/tmp/escape.json\"]",
            "[\"--jvmArgsAppend\",\"-Xmx100g\"]", "[\"--rff\",\"escape.json\"]", "[\"-foe\",\"false\"]",
            "[\"-o\",\"escape.log\"]", "[\"-f\",\"0\"]", "[\"-t\",\"max\"]", "[\"-l\"]",
            "[\"-prof\",\"jfr:dir=/tmp/out\"]", "[\"-p\",\"concurrency=None\",\"-t\",\"4\"]",
        )) assertFailsWith<IllegalArgumentException>(argv) { plan("backends" to "scalar", "argv" to argv) }
    }

    @Test fun `native executable paths cannot escape prepared Linux outputs`() {
        for (path in listOf("/tmp/bench.kexe", "../bench.kexe", "kumulant-bench/build/../linuxX64/bench.kexe",
            "kumulant-bench/build/bin/macosArm64/bench.kexe", "kumulant-bench/build/linuxX64/a\\b.kexe")) {
            assertFailsWith<IllegalArgumentException>(path) { plan("backends" to "native", "native-executable" to path) }
        }
    }
    @Test fun `failed harness runs retain completed metrics and nested raw artifacts`() {
        val root = Files.createTempDirectory("partial-kumulant")
        try {
            val module = Files.createDirectories(root.resolve("kumulant-bench"))
            Files.writeString(module.resolve("benchmark.sh"), """
                #!/bin/bash
                while ((${'$'}#)); do
                    if [[ ${'$'}1 == --output ]]; then output=${'$'}2; shift; fi
                    shift
                done
                mkdir -p "${'$'}output/pass-1"
                echo '[{"benchmark":"update","mode":"thrpt","primaryMetric":{"score":12.0,"scoreUnit":"ops/s"}}]' > "${'$'}output/pass-1/results.json"
                echo raw-sample > "${'$'}output/pass-1/samples.txt"
                echo failure-log
                exit 7
            """.trimIndent())
            val case = plan("backends" to "scalar", "repetitions" to "2").single()

            val result = KumulantLabSuite.run(case, BenchContext(root.resolve("out"), worktree = root))

            assertEquals(BenchStatus.FAILED, result.status)
            assertEquals(12.0, result.metrics.single().value)
            assertTrue(result.artifacts.map { it.path }.containsAll(listOf("benchmark.log", "report/pass-1/results.json", "report/pass-1/samples.txt")))
            assertTrue(Files.readString(root.resolve("out/benchmark.log")).contains("failure-log"))
        } finally { root.toFile().deleteRecursively() }
    }

}

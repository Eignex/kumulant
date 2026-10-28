package com.eignex.kumulant.bench

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CholeskyNativeLabProfileTest {
    @Test fun `failed native cases retain earlier samples and store their partial report`() {
        val root = Files.createTempDirectory("native prepared ")
        try {
            val quality = root.resolve("quality.kexe").toFile().apply {
                writeText("#!/bin/bash\necho 'Cholesky backend: KoblasEngine(scalar/)'\n")
                setExecutable(true)
            }
            val executable = root.resolve("bench.kexe").toFile().apply {
                writeText("""
                    #!/bin/bash
                    case ${'$'}2 in
                    --list) echo first > "${'$'}4/a.txt"; echo second > "${'$'}4/b.txt" ;;
                    --benchmark)
                        echo '1.0, 2.0' > "${'$'}5"
                        if [[ ${'$'}4 == */b.txt ]]; then echo failure-log; exit 7; fi ;;
                    --store-results)
                        while IFS= read -r line || [[ -n ${'$'}line ]]; do
                            case ${'$'}line in reportFile:*) report=${'$'}{line#reportFile:} ;; esac
                        done < "${'$'}1"
                        echo '[{"benchmark":"first","mode":"avgt","primaryMetric":{"score":1.5,"scoreUnit":"us/op"}}]' > "${'$'}report" ;;
                    esac
                """.trimIndent() + "\n")
                setExecutable(true)
            }
            val output = root.resolve("out").toFile().apply { mkdir() }

            assertFailsWith<IllegalStateException> {
                runPreparedNativeCholesky(output, emptyList(), smoke = true, worktree = root.toFile(),
                    benchmarkPath = executable.name, qualityPath = quality.name)
            }

            val repetition = output.resolve("native-0")
            val combined = repetition.resolve("samples.txt").readText()
            assertTrue(combined.contains("/cases/a.txt: 1.0, 2.0"))
            assertTrue(!combined.endsWith("\n") && !combined.contains("/cases/b.txt"))
            assertEquals("1.0, 2.0\n", repetition.resolve("samples-1.txt").readText())
            assertTrue(repetition.resolve("benchmark.txt").readText().contains("failure-log"))
            assertEquals(1.5, labMeasurements(output.toPath()).metrics.single().value)
        } finally { root.toFile().deleteRecursively() }
    }
}

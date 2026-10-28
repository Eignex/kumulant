package com.eignex.kumulant.bench

import java.io.File

internal fun runPreparedNativeCholesky(
    output: File, arguments: List<String>, stressOrder: Int? = null, smoke: Boolean = false,
    qualityOnly: Boolean = false, repetitions: Int = if (smoke) 1 else 2,
    worktree: File = File("."),
    benchmarkPath: String = "kumulant-bench/build/benchmarks/linuxX64/classes/linuxX64Benchmark.kexe",
    qualityPath: String = "kumulant-bench/build/bin/linuxX64/choleskyQualityReleaseExecutable/choleskyQuality.kexe",
) {
    // Prepared archives omit Gradle state; the engine protocol keeps compilation in preparation.
    val settings = NativeBenchmarkSettings(arguments, smoke)
    require(repetitions > 0)
    val executable = worktree.resolve(benchmarkPath).absoluteFile
    if (!qualityOnly) check(executable.isFile && executable.setExecutable(true)) { "Native benchmark must be linked during preparation" }
    val quality = worktree.resolve(qualityPath).absoluteFile
    check(quality.isFile && quality.setExecutable(true)) { "Native quality executable must be linked during preparation" }
    val preflight = ProcessBuilder(quality.path).redirectErrorStream(true)
        .redirectOutput(File(output, "native-quality.txt")).start()
    check(preflight.waitFor() == 0) { "Native quality failed; inspect native-quality.txt" }
    check(File(output, "native-quality.txt").readText().contains("Cholesky backend: KoblasEngine(")) {
        "Native backend did not resolve; inspect native-quality.txt"
    }
    if (stressOrder != null) {
        val stress = ProcessBuilder(quality.path, "$stressOrder").redirectErrorStream(true)
            .redirectOutput(File(output, "native-stress.txt")).start()
        check(stress.waitFor() == 0) { "Native stress failed; inspect native-stress.txt" }
    }
    if (qualityOnly) return
    repeat(repetitions) { repetition ->
        val directory = File(output, "native-$repetition").apply { check(mkdir()) }
        val cases = File(directory, "cases").apply { check(mkdir()) }
        val progress = File(directory, "progress.txt").apply { writeText("") }
        val configuration = File(directory, "configuration.txt").apply {
            writeText((listOf(
                "name:main", "configurationName:main", "compilationMode:Release",
                "reportFile:${File(directory, "results.json").absolutePath}",
                "traceFormat:text", "reportFormat:json",
            ) + settings.configuration()).joinToString("\n"))
        }
        val invocation = File(directory, "run.txt")
        val log = File(directory, "benchmark.txt")
        fun execute(action: String, vararg args: String) {
            val command = listOf(executable.path, configuration.absolutePath, action, progress.absolutePath) + args
            invocation.appendText(command.joinToString("\n") + "\n\n")
            val process = ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log)).start()
            check(process.waitFor() == 0) { "Native $action failed; inspect $log" }
        }
        println("Starting prepared Native repetition $repetition")
        execute("--list", cases.absolutePath)
        val configurations = requireNotNull(cases.listFiles()).filter { it.extension == "txt" }.sortedBy { it.name }
        check(configurations.isNotEmpty()) { "No Native benchmark cases matched" }
        val results = File(directory, "samples.txt")
        results.writeText("")
        var failure: Throwable? = null
        try {
            configurations.forEachIndexed { index, case ->
                val samples = File(directory, "samples-$index.txt")
                execute("--benchmark", case.absolutePath, samples.absolutePath)
                check(samples.isFile && samples.readText().isNotBlank()) { "Native benchmark produced no samples: $case" }
                results.appendText((if (results.length() > 0) "\n" else "") + "${case.absolutePath}: ${samples.readText().trim()}")
                println("Prepared Native repetition $repetition: ${index + 1}/${configurations.size} cases")
            }
        } catch (error: Exception) {
            failure = error
            throw error
        } finally {
            if (results.length() > 0) {
                try {
                    execute("--store-results", results.absolutePath)
                } catch (storeFailure: Exception) {
                    if (failure == null) throw storeFailure else failure.addSuppressed(storeFailure)
                }
            }
        }
        check(File(directory, "results.json").isFile) { "Native report is missing" }
        println("Completed prepared Native repetition $repetition")
    }
}

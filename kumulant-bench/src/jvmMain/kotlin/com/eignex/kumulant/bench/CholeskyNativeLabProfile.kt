package com.eignex.kumulant.bench

import java.io.File

internal fun runPreparedNativeCholesky(output: File, arguments: List<String>, stressOrder: Int? = null, smoke: Boolean = false, qualityOnly: Boolean = false) {
    // Prepared archives omit Gradle state; the engine protocol keeps compilation in preparation.
    val executable = File("kumulant-bench/build/benchmarks/linuxX64/classes/linuxX64Benchmark.kexe").absoluteFile
    check(executable.isFile) { "Native benchmark must be linked during preparation" }
    check(executable.setExecutable(true)) { "Native benchmark must be executable" }
    val filters = arguments.filter { it.startsWith("-Pbench.filter=") }
    require(filters.size <= 1)
    val selection = filters.singleOrNull()
        ?.substringAfter('=') ?: ".*Cholesky.*Benchmark.*"
    val parameters = arguments.filter { it.startsWith("-Pbench.param.") }.flatMap { argument ->
        val name = argument.removePrefix("-Pbench.param.").substringBefore('=')
        argument.substringAfter('=').split(',').map { "param:$name=$it" }
    }
    require(arguments.all { it.startsWith("-Pbench.filter=") || it.startsWith("-Pbench.param.") })
    require(arguments.none { '\n' in it || '\r' in it })
    val quality = File("kumulant-bench/build/bin/linuxX64/choleskyQualityReleaseExecutable/choleskyQuality.kexe").absoluteFile
    check(quality.isFile && quality.setExecutable(true)) { "Native quality executable must be linked during preparation" }
    val preflight = ProcessBuilder(quality.path).redirectErrorStream(true)
        .redirectOutput(File(output, "native-quality.txt")).start()
    check(preflight.waitFor() == 0) { "Native quality failed; inspect native-quality.txt" }
    if (stressOrder != null) {
        val stress = ProcessBuilder(quality.path, "$stressOrder").redirectErrorStream(true)
            .redirectOutput(File(output, "native-stress.txt")).start()
        check(stress.waitFor() == 0) { "Native stress failed; inspect native-stress.txt" }
    }
    if (qualityOnly) return
    repeat(if (smoke) 1 else 2) { repetition ->
        val directory = File(output, "native-$repetition").apply { check(mkdir()) }
        val cases = File(directory, "cases").apply { check(mkdir()) }
        val progress = File(directory, "progress.txt").apply { writeText("") }
        val configuration = File(directory, "configuration.txt").apply {
            writeText((listOf(
                "name:main", "configurationName:main", "compilationMode:Release",
                "reportFile:${File(directory, "results.json").absolutePath}",
                "traceFormat:text", "reportFormat:json", "warmups:${if (smoke) 1 else 3}", "iterations:${if (smoke) 2 else 5}",
                "iterationTime:${if (smoke) 100 else 1000}", "iterationTimeUnit:ms", "outputTimeUnit:us", "mode:avgt",
                "include:$selection",
            ) + parameters).joinToString("\n"))
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
        val samplesByCase = mutableListOf<String>()
        configurations.forEachIndexed { index, case ->
            val samples = File(directory, "samples-$index.txt")
            execute("--benchmark", case.absolutePath, samples.absolutePath)
            check(samples.isFile && samples.readText().isNotBlank()) { "Native benchmark produced no samples: $case" }
            samplesByCase.add("${case.absolutePath}: ${samples.readText()}")
            println("Prepared Native repetition $repetition: ${index + 1}/${configurations.size} cases")
        }
        results.writeText(samplesByCase.joinToString("\n"))
        execute("--store-results", results.absolutePath)
        check(File(directory, "results.json").isFile) { "Native report is missing" }
        println("Completed prepared Native repetition $repetition")
    }
}

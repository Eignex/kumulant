package com.eignex.kumulant.bench

import java.io.File

fun main(arguments: Array<String>) {
    val stressOrder = arguments.singleOrNull { it.startsWith("--stress-order=") }?.substringAfter('=')?.toInt()
    val args = arguments.filterNot { it.startsWith("--stress-order=") }
    val output = File(requireNotNull(System.getenv("OUT")) { "Eignex Lab supplies OUT" }, "report")
    check(output.mkdirs()) { "Report directory already exists: $output" }
    File("kumulant-bench/build/benchmark-dependencies.txt").copyTo(File(output, "dependencies.txt"))
    val java = File(System.getProperty("java.home"), "bin/java").absolutePath
    val classpath = System.getProperty("java.class.path")
    val hardware = ProcessBuilder("lscpu").redirectErrorStream(true).start()
    File(output, "host.txt").writeText(hardware.inputStream.bufferedReader().readText())
    check(hardware.waitFor() == 0)
    if (args.firstOrNull() == "--native-prepared") {
        runPreparedNativeCholesky(output, args.drop(1), stressOrder)
        return
    }
    if (args.firstOrNull() == "--native") {
        repeat(2) { repetition ->
            val directory = File(output, "native-$repetition").apply { check(mkdir()) }
            val command = listOf(
                "./gradlew", ":kumulant-bench:linuxX64Benchmark", "--console=plain", "--no-daemon",
                "-Dorg.gradle.jvmargs=-Xmx2g -XX:MaxMetaspaceSize=512m",
                "-Pbench.filter=.*Cholesky.*Benchmark.*", "-Pbench.mode=avgt", "-Pbench.timeUnit=us",
                "-Pbench.output=${directory.absolutePath}",
            ) + args.drop(1)
            File(directory, "run.txt").writeText(command.joinToString("\n"))
            println("Starting Native repetition $repetition")
            val process = ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(File(directory, "benchmark.txt")).start()
            check(process.waitFor() == 0) { "Native repetition $repetition failed" }
            println("Completed Native repetition $repetition")
        }
        return
    }
    val recording = "--jfr" in args
    val requested = args.filterNot { it == "--jfr" || it == "--simd-only" }
    val selection = if (requested.firstOrNull() == "--filter") requested[1] else ".*Cholesky.*Benchmark.*"
    val parameters = if (requested.firstOrNull() == "--filter") requested.drop(2) else requested
    val backends = if ("--simd-only" in args) listOf("simd") else listOf("scalar", "simd")
    for (backend in backends) {
        val directory = File(output, backend).apply { check(mkdir()) }
        val command = listOf(java, "--enable-native-access=ALL-UNNAMED", "-Xms512m", "-Xmx512m") +
            (if (backend == "simd") listOf("--add-modules=jdk.incubator.vector") else emptyList()) +
            listOf("-cp", classpath, "com.eignex.kumulant.bench.BenchmarkMainKt")
        File(directory, "run.txt").writeText(
            "java=${System.getProperty("java.runtime.version")}\nos=${System.getProperty("os.name")}\n" +
                "command=$command\narguments=${arguments.toList()}\nload=${File("/proc/loadavg").readText()}",
        )
        println("Starting $backend quality and measurements")
        val quality = ProcessBuilder(command + "--quality").redirectErrorStream(true)
            .redirectOutput(File(directory, "quality.txt")).start()
        check(quality.waitFor() == 0) { "$backend quality failed" }
        val expected = if (backend == "simd") "KoblasEngine(simd(" else "KoblasEngine(scalar/"
        check(File(directory, "quality.txt").readText().contains(expected)) {
            "$backend backend did not resolve; inspect quality.txt"
        }
        if (stressOrder != null) {
            val stress = ProcessBuilder(command + listOf("--cholesky-stress", "$stressOrder")).redirectErrorStream(true)
                .redirectOutput(File(directory, "stress.txt")).start()
            check(stress.waitFor() == 0) { "$backend stress failed; inspect stress.txt" }
            check(File(directory, "stress.txt").readText().contains(expected)) { "$backend stress backend did not resolve" }
        }
        val profiler = if (recording) listOf("-prof", "jfr:dir=${File(directory, "jfr").absolutePath}") else emptyList()
        val benchmark = ProcessBuilder(command + listOf(selection, "-bm", "avgt", "-tu", "us", "-prof", "gc") + profiler +
            parameters + listOf("-rf", "json", "-rff", File(directory, "results.json").absolutePath))
            .redirectErrorStream(true).redirectOutput(File(directory, "benchmark.txt")).start()
        check(benchmark.waitFor() == 0) { "$backend benchmark failed" }
        println("Completed $backend")
    }
}

package com.eignex.kumulant.bench

import java.io.File

fun main(args: Array<String>) {
    val output = File(requireNotNull(System.getenv("OUT")) { "Eignex Lab supplies OUT" }, "report")
    check(output.mkdirs()) { "Report directory already exists: $output" }
    val java = File(System.getProperty("java.home"), "bin/java").absolutePath
    val classpath = System.getProperty("java.class.path")
    val hardware = ProcessBuilder("lscpu").redirectErrorStream(true).start()
    File(output, "host.txt").writeText(hardware.inputStream.bufferedReader().readText())
    check(hardware.waitFor() == 0)
    for (backend in listOf("scalar", "simd")) {
        val directory = File(output, backend).apply { check(mkdir()) }
        val command = listOf(java, "--enable-native-access=ALL-UNNAMED", "-Xms512m", "-Xmx512m") +
            (if (backend == "simd") listOf("--add-modules=jdk.incubator.vector") else emptyList()) +
            listOf("-cp", classpath, "com.eignex.kumulant.bench.BenchmarkMainKt")
        File(directory, "run.txt").writeText(
            "java=${System.getProperty("java.runtime.version")}\nos=${System.getProperty("os.name")}\n" +
                "command=$command\narguments=${args.toList()}\nload=${File("/proc/loadavg").readText()}",
        )
        println("Starting $backend quality and measurements")
        val quality = ProcessBuilder(command + "--quality").redirectErrorStream(true)
            .redirectOutput(File(directory, "quality.txt")).start()
        check(quality.waitFor() == 0) { "$backend quality failed" }
        val benchmark = ProcessBuilder(command + listOf(".*Cholesky.*Benchmark.*", "-bm", "avgt", "-tu", "us", "-prof", "gc") +
            args + listOf("-rf", "json", "-rff", File(directory, "results.json").absolutePath))
            .redirectErrorStream(true).redirectOutput(File(directory, "benchmark.txt")).start()
        check(benchmark.waitFor() == 0) { "$backend benchmark failed" }
        println("Completed $backend")
    }
}

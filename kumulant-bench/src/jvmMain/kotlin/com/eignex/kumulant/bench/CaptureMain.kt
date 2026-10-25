package com.eignex.kumulant.bench

import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString

@Serializable
internal data class CaptureOptions(
    val preset: String = "standard",
    val suite: String? = null,
    val pattern: String = "*",
    val mode: String = "performance",
    val targets: List<String> = listOf("jvm-scalar", "jvm-simd"),
    val allocation: Boolean = false,
    val output: String? = null,
    val list: Boolean = false,
)

@Serializable
internal data class CaptureTiming(
    val warmups: Int,
    val iterations: Int,
    val iterationMillis: Int,
    val forks: Int,
    val threads: Int = 1,
    val mode: String = "avgt",
    val unit: String = "ns/op",
)

internal fun captureTiming(preset: String): CaptureTiming = when (preset) {
    "smoke" -> CaptureTiming(1, 2, 100, 1)
    "standard" -> CaptureTiming(3, 5, 1000, 2)
    "extended" -> CaptureTiming(5, 10, 1000, 3)
    else -> error("Unknown preset: $preset")
}

internal fun captureOptions(args: Array<String>): CaptureOptions {
    var options = CaptureOptions()
    var index = 0
    val seen = mutableSetOf<String>()
    while (index < args.size) {
        val option = args[index++]
        require(seen.add(option)) { "Repeated option: $option" }
        if (option == "--allocation") { options = options.copy(allocation = true); continue }
        if (option == "--list") { options = options.copy(list = true); continue }
        require(option in listOf("--preset", "--suite", "--case", "--mode", "--targets", "--output")) {
            "Unknown option: $option; see capture-report.sh --help"
        }
        require(index < args.size && !args[index].startsWith("--")) { "Missing value for $option" }
        val value = args[index++]
        options = when (option) {
            "--preset" -> options.copy(preset = value)
            "--suite" -> options.copy(suite = value)
            "--case" -> options.copy(pattern = value)
            "--mode" -> options.copy(mode = value)
            "--targets" -> options.copy(targets = value.split(','))
            else -> options.copy(output = value)
        }
    }
    require(options.mode in listOf("quality", "performance")) { "Mode must be quality or performance" }
    require(options.targets.isNotEmpty() && options.targets.all { it in listOf("jvm-scalar", "jvm-simd") }) {
        "Supported targets: jvm-scalar,jvm-simd"
    }
    require(options.targets.distinct().size == options.targets.size) { "Targets must be unique" }
    require(options.output == null || options.output.isNotBlank()) { "Output directory must not be blank" }
    require(!options.allocation || options.mode != "quality") { "Allocation profiling requires performance mode" }
    return options
}

fun main(args: Array<String>) {
    val options = captureOptions(args)
    val cases = selectCaptureCases(options.preset, options.suite, options.pattern)
    if (options.list) {
        println(captureJson.encodeToString(cases))
        return
    }
    val output = Path.of(options.output ?: "kumulant-bench/build/captures/${UUID.randomUUID()}").toAbsolutePath().normalize()
    prepareCaptureOutput(output)
    writeCaptureManifest(output, options, cases)
    val results = cases.flatMap { case -> options.targets.map { target -> CaptureResult(case, target, "pending") } }.toMutableList()
    writeCaptureResults(output, results, "running")
    println("Capture: $output")
    if (options.preset == "smoke") println("Smoke timings verify wiring; they are not a performance baseline.")
    val timing = captureTiming(options.preset)
    try {
        for (index in results.indices) {
            val result = results[index]
            println("${index + 1}/${results.size}: ${result.case.id} (${result.target})")
            results[index] = measureCaptureCase(output, options, timing, result)
            writeCaptureResults(output, results, "running")
        }
    } finally {
        val status = when {
            results.any { it.status == "pending" } -> "incomplete"
            results.any { it.status == "failed" } -> "failed"
            else -> "passed"
        }
        writeCaptureResults(output, results, status)
    }
    check(results.all { it.status == "passed" }) { "Capture failed; see $output/summary.md and raw logs" }
    println("Capture passed: $output/summary.md")
}

internal fun prepareCaptureOutput(output: Path) {
    if (Files.exists(output)) {
        require(Files.isDirectory(output)) { "Output is not a directory: $output" }
        Files.list(output).use { require(it.findAny().isEmpty) { "Output directory must be empty: $output" } }
    } else {
        Files.createDirectories(output)
    }
    Files.createDirectory(output.resolve("raw"))
}

private fun measureCaptureCase(
    output: Path,
    options: CaptureOptions,
    timing: CaptureTiming,
    result: CaptureResult,
): CaptureResult {
    val stem = result.case.id.replace('/', '-') + "-${result.target}"
    val validationFile = output.resolve("raw/$stem-quality.json")
    var validation: CaptureValidation? = null
    var raw: String? = null
    return try {
        val quality = captureJavaCommand(result.target) + listOf(
            "com.eignex.kumulant.bench.CaptureQualityKt", options.preset, result.case.id, result.target, validationFile.toString(),
        )
        val exit = runCaptureProcess(quality, output.resolve("raw/$stem-quality"))
        if (Files.exists(validationFile)) validation = captureJson.decodeFromString(Files.readString(validationFile))
        check(exit == 0 && validation?.passed == true) { "Quality check failed; see raw/$stem-quality.log" }
        val metrics = if (options.mode == "quality") emptyList() else {
            raw = "raw/$stem-jmh.json"
            val command = captureJavaCommand(result.target) + captureJmhArguments(result.case, timing, output.resolve(raw), options.allocation)
            check(runCaptureProcess(command, output.resolve("raw/$stem-jmh")) == 0) {
                "JMH failed; see raw/$stem-jmh.log"
            }
            jmhMetrics(Files.readString(output.resolve(raw)), result.case)
        }
        result.copy(status = "passed", validation = validation, metrics = metrics, raw = raw)
    } catch (failure: Exception) {
        System.err.println("${result.case.id} (${result.target}): ${failure.message}")
        result.copy(status = "failed", validation = validation, raw = raw, reason = failure.toString())
    }
}

internal fun captureJmhArguments(case: CaptureCase, timing: CaptureTiming, output: Path, allocation: Boolean): List<String> = buildList {
    add("org.openjdk.jmh.Main")
    add("^${Regex.escape("com.eignex.kumulant.bench.${case.benchmark}")}$")
    addAll(listOf("-bm", timing.mode, "-tu", "ns", "-wi", timing.warmups.toString(), "-i", timing.iterations.toString()))
    addAll(listOf("-w", "${timing.iterationMillis}ms", "-r", "${timing.iterationMillis}ms", "-f", timing.forks.toString()))
    addAll(listOf("-t", timing.threads.toString(), "-foe", "true", "-rf", "json", "-rff", output.toString()))
    for ((name, value) in case.parameters) addAll(listOf("-p", "$name=$value"))
    if (allocation) addAll(listOf("-prof", "gc"))
}

private fun captureJavaCommand(target: String): List<String> = buildList {
    add(Path.of(System.getProperty("java.home"), "bin", "java").toString())
    addAll(listOf("--enable-native-access=ALL-UNNAMED", "-Xms512m", "-Xmx512m"))
    if (target == "jvm-simd") add("--add-modules=jdk.incubator.vector")
    addAll(listOf("-cp", System.getProperty("java.class.path")))
}

private fun runCaptureProcess(command: List<String>, output: Path): Int {
    writeCaptureJson(output.resolveSibling("${output.fileName}.command.json"), captureJson.encodeToString(command))
    val process = ProcessBuilder(command).redirectErrorStream(true)
        .redirectOutput(output.resolveSibling("${output.fileName}.log").toFile()).start()
    val cleanup = Thread { terminateCaptureProcess(process) }
    Runtime.getRuntime().addShutdownHook(cleanup)
    try {
        check(process.waitFor(120, TimeUnit.SECONDS)) { "Measurement timed out; see $output.log" }
        return process.exitValue()
    } finally {
        terminateCaptureProcess(process)
        Runtime.getRuntime().removeShutdownHook(cleanup)
    }
}

private fun terminateCaptureProcess(process: Process) {
    process.descendants().use { descendants -> descendants.forEach { it.destroyForcibly() } }
    if (process.isAlive) process.destroyForcibly()
}

private fun writeCaptureManifest(output: Path, options: CaptureOptions, cases: List<CaptureCase>) {
    val classpath = System.getProperty("java.class.path").split(System.getProperty("path.separator")).map(Path::of)
    val manifest = CaptureManifest(
        startedAt = Instant.now().toString(),
        revision = captureGit("rev-parse", "HEAD").trim(),
        workingTreeStatus = captureGit("status", "--porcelain"),
        diffSha256 = captureSha256(captureGit("diff", "--binary", "HEAD").toByteArray()),
        harnessSha256 = classpath.associate { it.fileName.toString() to captureSha256(Files.readAllBytes(it)) },
        dependencies = Files.readAllLines(Path.of("kumulant-bench/build/capture/dependencies.txt")),
        jmhVersion = Properties().apply {
            ClassLoader.getSystemResourceAsStream("META-INF/maven/org.openjdk.jmh/jmh-core/pom.properties")!!.use { load(it) }
        }.getProperty("version"),
        runtime = mapOf(
            "javaVersion" to System.getProperty("java.runtime.version"),
            "javaVendor" to System.getProperty("java.vendor"),
            "javaHome" to System.getProperty("java.home"),
            "javaArguments" to ManagementFactory.getRuntimeMXBean().inputArguments.joinToString(" "),
            "os" to "${System.getProperty("os.name")} ${System.getProperty("os.version")}",
            "architecture" to System.getProperty("os.arch"),
            "availableProcessors" to Runtime.getRuntime().availableProcessors().toString(),
            "cpu" to captureHostFile("/proc/cpuinfo").lineSequence().filter {
                it.substringBefore(':').trim() in listOf("vendor_id", "model name", "cpu family", "model", "stepping", "flags", "Hardware", "CPU implementer", "CPU part")
            }.distinct().joinToString("\n"),
            "memory" to captureHostFile("/proc/meminfo").lineSequence().firstOrNull { it.startsWith("MemTotal:") }.orEmpty(),
            "affinity" to captureHostFile("/proc/self/status").lineSequence().filter { it.startsWith("Cpus_allowed_list:") }.joinToString(),
        ),
        options = options,
        timing = captureTiming(options.preset),
        cases = cases,
    )
    writeCaptureJson(output.resolve("manifest.json"), captureJson.encodeToString(manifest))
}

@Serializable
private data class CaptureManifest(
    val schemaVersion: Int = 1,
    val startedAt: String,
    val revision: String,
    val workingTreeStatus: String,
    val diffSha256: String,
    val harnessSha256: Map<String, String>,
    val dependencies: List<String>,
    val jmhVersion: String,
    val runtime: Map<String, String>,
    val options: CaptureOptions,
    val timing: CaptureTiming,
    val cases: List<CaptureCase>,
    val instrumented: Boolean = options.allocation,
)

private fun captureGit(vararg args: String): String {
    val process = ProcessBuilder(listOf("git") + args).redirectErrorStream(true).start()
    val output = process.inputStream.bufferedReader().use { it.readText() }
    check(process.waitFor() == 0) { "git ${args.first()} failed: $output" }
    return output
}

private fun captureHostFile(path: String): String =
    if (Files.isReadable(Path.of(path))) Files.readString(Path.of(path)) else "unavailable"

private fun captureSha256(bytes: ByteArray): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

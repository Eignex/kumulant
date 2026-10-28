package com.eignex.kumulant.bench

import com.eignex.lab.bench.*
import java.nio.file.Files
import java.nio.file.Path
import kotlinx.serialization.json.*
import org.openjdk.jmh.runner.options.CommandLineOptions

fun main(args: Array<String>) = BenchCli.execute(KumulantLabSuite, args)

internal object KumulantLabSuite : BenchSuite {
    private val options = setOf("backends", "mode", "smoke", "argv", "memory-mib", "timeout-seconds",
        "repetitions", "stress-order", "heap-mib", "jvm-gc", "jfr", "native-executable", "native-quality-executable")
    private const val NATIVE_BENCHMARK = "kumulant-bench/build/benchmarks/linuxX64/classes/linuxX64Benchmark.kexe"
    private const val NATIVE_QUALITY = "kumulant-bench/build/bin/linuxX64/choleskyQualityReleaseExecutable/choleskyQuality.kexe"

    override fun describe() = BenchDescriptor("kumulant-bench", "Prepared quality, concurrency and JVM/Native benchmark engines with raw reports")

    private fun arguments(value: String?): List<String> = value?.let {
        val array = BenchProtocol.json.parseToJsonElement(it)
        require(array is JsonArray) { "argv must encode a JSON string array" }
        array.map { element ->
            require(element is JsonPrimitive && element.isString && element.content.none(Char::isISOControl)) { "argv must contain literal strings" }
            element.content
        }
    } ?: emptyList()

    private fun positive(p: Map<String, String>, key: String, default: Int): Int =
        (p[key] ?: default.toString()).toIntOrNull()?.also { require(it > 0) { "$key must be positive" } }
            ?: throw IllegalArgumentException("$key must be a positive integer")

    override fun plan(request: BenchRequest): List<BenchCase> {
        val p = request.parameters
        require(p.keys.all { it in options }) { "Unknown Kumulant bench parameter: ${p.keys - options}" }
        require(p.values.none { it.isBlank() || it.any(Char::isISOControl) }) { "Parameters must be nonblank literal values" }
        val mode = p["mode"] ?: "performance"
        require(mode in setOf("performance", "quality", "concurrency")) { "Unsupported mode: $mode" }
        val smoke = p["smoke"]?.toBooleanStrict() ?: false
        val jfr = p["jfr"]?.toBooleanStrict() ?: false
        val memory = positive(p, "memory-mib", 2048)
        require(memory >= 2048) { "memory-mib must be at least 2048" }
        val timeout = positive(p, "timeout-seconds", 3600)
        val backends = (p["backends"] ?: "scalar,simd").split(',')
        require(backends.distinct().size == backends.size && backends.all { it in setOf("scalar", "simd", "native") }) { "Unknown or duplicate backend" }
        val args = arguments(p["argv"])
        require(mode == "performance" || args.isEmpty()) { "argv is only used by performance mode" }
        require(!jfr || mode == "performance" && "native" !in backends) { "JFR requires JVM performance" }
        p["stress-order"]?.let { require(positive(p, "stress-order", 0) > 16) { "stress-order must exceed 16" } }
        require("native" in backends || p.keys.none { it.startsWith("native-") }) { "Native executable selection requires Native" }
        require(backends.any { it != "native" } || p.keys.none { it in setOf("heap-mib", "jvm-gc") }) { "JVM controls require a JVM backend" }
        return backends.map { backend ->
            require(backend != "native" || mode != "concurrency") { "Concurrency checks are JVM only" }
            val repetitions = positive(p, "repetitions", if (backend == "native" && !smoke) 2 else 1)
            val parameters = mutableMapOf("backend" to backend, "mode" to mode, "smoke" to smoke.toString(), "repetitions" to repetitions.toString())
            p["stress-order"]?.let { parameters["stress-order"] = it }
            val inputs = mutableListOf<BenchInput>()
            var cpus = if (mode == "concurrency") 5 else 1
            if (backend == "native") {
                parameters["argv"] = JsonArray(NativeBenchmarkSettings(args, smoke).arguments().map(::JsonPrimitive)).toString()
                for ((key, default) in listOf("native-executable" to NATIVE_BENCHMARK, "native-quality-executable" to NATIVE_QUALITY)) {
                    val path = p[key] ?: default
                    val input = BenchInput(path, role = "executable")
                    require(path.startsWith("kumulant-bench/build/") && "/linuxX64/" in path && path.endsWith(".kexe")) { "Native executable must be prepared for linuxX64 under kumulant-bench/build" }
                    parameters[key] = path
                    inputs += input
                }
            } else {
                val heap = positive(p, "heap-mib", 512)
                // The harness and its measured fork can both hold the requested heap.
                require(heap >= 64 && heap.toLong() * 2 + 768 <= memory) { "heap-mib exceeds the two-JVM memory budget" }
                val gc = p["jvm-gc"] ?: "g1"
                require(gc in setOf("g1", "serial", "zgc")) { "jvm-gc must be g1, serial or zgc" }
                parameters += mapOf("heap-mib" to heap.toString(), "jvm-gc" to gc, "jfr" to jfr.toString())
                val frozen = if (mode == "performance") jvmArguments(args, smoke) else emptyList()
                if (mode == "performance") cpus = CommandLineOptions(*frozen.toTypedArray()).threads.orElse(1)
                parameters["argv"] = JsonArray(frozen.map(::JsonPrimitive)).toString()
            }
            BenchCase("$backend/$mode", parameters, BenchResources(cpus, memory.toLong(), timeout.toLong(),
                if (backend == "native") setOf("platform:linux", "arch:x86_64") else emptySet()), inputs = inputs)
        }
    }

    private fun jvmArguments(args: List<String>, smoke: Boolean): List<String> {
        val owned = setOf("-rff", "-rf", "-o", "-jvm", "-jvmArgs", "-jvmArgsAppend", "-jvmArgsPrepend", "-h", "-l", "-lp", "-lprof", "-lrf", "-tg")
        require(args.none { it.substringBefore('=') in owned || it.startsWith("-P") }) { "Adapter owns report paths, JVM flags and launch actions; use JVM controls or JMH measurement arguments" }
        val cli = CommandLineOptions(*args.toTypedArray())
        require(!cli.jvm.hasValue() && !cli.jvmArgs.hasValue() && !cli.jvmArgsAppend.hasValue() && !cli.jvmArgsPrepend.hasValue() &&
            !cli.output.hasValue() && !cli.result.hasValue() && !cli.resultFormat.hasValue() && !cli.threadGroups.hasValue() &&
            !cli.shouldHelp() && !cli.shouldList() && !cli.shouldListWithParams() && !cli.shouldListProfilers() && !cli.shouldListResultFormats()) {
            "Adapter owns JVM flags, report paths and launch actions, including JMH option aliases"
        }
        require(cli.shouldFailOnError().orElse(true)) { "Prepared benchmarks require fail-on-error" }
        val threads = cli.threads.orElse(1)
        require(threads > 0) { "Use an explicit positive JMH thread count" }
        require(cli.forkCount.orElse(1) > 0) { "Prepared benchmarks require at least one JMH fork" }
        require(cli.profilers.all { it.klass in setOf("gc", "stack", "safepoints", "comp", "cl") && !it.opts.contains(Regex("[/\\\\]|dir=")) }) {
            "Use portable JMH profilers gc, stack, safepoints, comp or cl; use jfr=true for retained recordings"
        }
        val defaults = benchmarkOptions(cli, smoke)
        return args + buildList {
            if (cli.includes.isEmpty() && threads == 1) addAll(defaults.includes)
            for (name in listOf("name", "concurrency", "policy")) if (!cli.getParameter(name).hasValue()) {
                add("-p"); add("$name=${defaults.getParameter(name).get().joinToString(",")}")
            }
            if (!cli.threads.hasValue()) addAll(listOf("-t", defaults.threads.get().toString()))
            if (!cli.forkCount.hasValue()) addAll(listOf("-f", defaults.forkCount.get().toString()))
            if (!cli.warmupIterations.hasValue()) addAll(listOf("-wi", defaults.warmupIterations.get().toString()))
            if (!cli.measurementIterations.hasValue()) addAll(listOf("-i", defaults.measurementIterations.get().toString()))
            if (!cli.warmupTime.hasValue()) addAll(listOf("-w", defaults.warmupTime.get().toString().replace(" ", "")))
            if (!cli.measurementTime.hasValue()) addAll(listOf("-r", defaults.measurementTime.get().toString().replace(" ", "")))
        }
    }

    override fun run(case: BenchCase, context: BenchContext): BenchResult {
        val p = case.parameters
        val backend = p.getValue("backend")
        // Revalidate the frozen command and require the files that preparation inventoried.
        val request = p - "backend" + mapOf("backends" to backend,
            "memory-mib" to case.resources.memoryMiB.toString(), "timeout-seconds" to case.resources.timeoutSeconds.toString())
        val replay = plan(BenchRequest(if (p["mode"] == "performance") request else request - "argv")).single()
        require(replay.resources == case.resources && replay.inputs.all { expected -> case.inputs.any { it.path == expected.path && it.root == expected.root && it.role == expected.role } }) { "Case resources or declared inputs do not match the frozen workload" }
        val root = requireNotNull(context.worktree) { "Prepared execution requires LAB_WORKTREE" }.toAbsolutePath()
        val report = context.reportPath()
        val errors = mutableListOf<String>()
        runCatching {
            if (backend == "native") {
                require(System.getProperty("os.name") == "Linux" && System.getProperty("os.arch") in setOf("amd64", "x86_64")) { "Native requires Linux x86_64" }
                Files.createDirectories(report)
                val preflight = listOf("bash", root.resolve("kumulant-bench/benchmark.sh").toString(), "--prepared", "--scalar", "--quality", "--output", report.resolve("jvm-quality").toString())
                check(context.process(preflight, "benchmark.log", environment = mapOf(
                    "KUMULANT_BENCH_JAVA" to Path.of(System.getProperty("java.home"), "bin", "java").toString())) == 0) {
                    "JVM reference quality failed before Native execution; inspect benchmark.log and jvm-quality"
                }
                root.resolve("kumulant-bench/build/benchmark-dependencies.txt").takeIf(Files::isRegularFile)?.let { Files.copy(it, report.resolve("dependencies.txt")) }
                Files.writeString(report.resolve("runtime.txt"), "java=${System.getProperty("java.runtime.version")}\nos=${System.getProperty("os.name")}/${System.getProperty("os.arch")}\nparameters=$p\n")
                repeat(p.getValue("repetitions").toInt().takeIf { p["mode"] == "quality" } ?: 1) { repetition ->
                    val output = if (p["mode"] == "quality") Files.createDirectories(report.resolve("pass-${repetition + 1}")) else report
                    runPreparedNativeCholesky(output.toFile(), arguments(p["argv"]), p["stress-order"]?.toInt(),
                        p.getValue("smoke").toBooleanStrict(), p["mode"] == "quality", p.getValue("repetitions").toInt(), root.toFile(),
                        p.getValue("native-executable"), p.getValue("native-quality-executable"))
                }
            } else {
                val argv = mutableListOf("bash", root.resolve("kumulant-bench/benchmark.sh").toString(), "--prepared", "--output", report.toString(),
                    "--repetitions", p.getValue("repetitions"), "--heap-mib", p.getValue("heap-mib"), "--jvm-gc", p.getValue("jvm-gc"))
                if (backend == "scalar") argv += "--scalar"
                if (p.getValue("smoke").toBooleanStrict()) argv += "--smoke"
                if (p.getValue("jfr").toBooleanStrict()) argv += "--jfr"
                p["stress-order"]?.let { argv += listOf("--stress-order", it) }
                when (p.getValue("mode")) { "quality" -> argv += "--quality"; "concurrency" -> argv += "--concurrency" }
                argv += "--"; argv += arguments(p["argv"])
                val exit = context.process(argv, "benchmark.log", environment = mapOf(
                    "KUMULANT_BENCH_JAVA" to Path.of(System.getProperty("java.home"), "bin", "java").toString()))
                check(exit == 0) { "Benchmark harness failed (exit $exit); inspect benchmark.log and reports" }
            }
        }.exceptionOrNull()?.let { errors += it.message ?: it.javaClass.name }
        val measurements = labMeasurements(report)
        errors += measurements.errors
        if (p["mode"] == "performance" && measurements.primaryCount == 0) errors += "No finite benchmark measurements were reported"
        val artifacts = context.artifacts() + if (Files.isRegularFile(context.outputDirectory.resolve("benchmark.log")))
            listOf(context.artifact("benchmark.log", "text/plain")) else emptyList()
        return BenchResult(case.id, if (errors.isEmpty()) BenchStatus.SUCCESS else BenchStatus.FAILED,
            measurements.metrics, artifacts, details = JsonObject(measurements.details + ("parameters" to JsonObject(p.mapValues { JsonPrimitive(it.value) }))), message = errors.takeIf { it.isNotEmpty() }?.joinToString("; "))
    }
}

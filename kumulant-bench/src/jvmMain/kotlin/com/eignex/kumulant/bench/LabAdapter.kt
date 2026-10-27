package com.eignex.kumulant.bench

import com.eignex.lab.bench.*
import java.nio.file.Files
import java.nio.file.Path
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.openjdk.jmh.runner.options.CommandLineOptions

fun main(args: Array<String>) = BenchCli.execute(KumulantLabSuite, args)

internal object KumulantLabSuite : BenchSuite {
    private val options = setOf("backends", "mode", "smoke", "argv", "memory-mib", "timeout-seconds")
    private val nativeInputs = listOf("kumulant-bench/build/benchmarks/linuxX64/classes/linuxX64Benchmark.kexe",
        "kumulant-bench/build/bin/linuxX64/choleskyQualityReleaseExecutable/choleskyQuality.kexe")

    override fun describe() = BenchDescriptor("kumulant-bench", "Prepared quality, concurrency and JVM/Native benchmark engines with raw reports")

    private fun arguments(value: String?): List<String> = value?.let {
        (BenchProtocol.json.parseToJsonElement(it) as JsonArray).map { element ->
            require(element is JsonPrimitive && element.isString)
            element.content
        }
    } ?: emptyList()

    override fun plan(request: BenchRequest): List<BenchCase> {
        val p = request.parameters
        require(p.keys.all { it in options }) { "Unknown Kumulant bench parameter" }
        val mode = p["mode"] ?: "performance"
        require(mode in setOf("performance", "quality", "concurrency"))
        require((p["memory-mib"] ?: "2048").toLong() >= 2048)
        val args = arguments(p["argv"])
        val backends = (p["backends"] ?: "scalar,simd").split(',').distinct()
        require(backends.all { it in setOf("scalar", "simd", "native") })
        p["smoke"]?.toBooleanStrict()
        return backends.map { backend ->
            require(backend != "native" || mode != "concurrency") { "Concurrency checks are JVM only" }
            val cpus = if (mode == "concurrency") 4 else if (backend == "native") 1
                else CommandLineOptions(*args.toTypedArray()).threads.orElse(1).also { require(it > 0) { "Use an explicit positive JMH thread count" } }
            if (backend == "native") require(args.all { it.startsWith("-Pbench.filter=") || it.startsWith("-Pbench.param.") })
            val frozen = if (backend == "native" && args.isEmpty()) {
                val smoke = p["smoke"]?.toBooleanStrict() ?: false
                val defaults = listOf("-Pbench.filter=.*(UpdateThroughputBenchmark.update|BanditBenchmark.cycle)",
                    "-Pbench.param.name=${if (smoke) "SumStat" else allSpecs.joinToString(",") { it.name }}",
                    "-Pbench.param.policy=${if (smoke) "MultiArmed/Thompson" else allBanditSpecs.joinToString(",") { it.name }}",
                    "-Pbench.param.concurrency=None")
                p + ("argv" to JsonArray(defaults.map(::JsonPrimitive)).toString())
            } else p
            BenchCase("$backend/$mode", frozen - setOf("backends", "memory-mib", "timeout-seconds") + mapOf("backend" to backend, "mode" to mode),
                BenchResources(cpus, (p["memory-mib"] ?: "2048").toLong(), (p["timeout-seconds"] ?: "3600").toLong(),
                    if (backend == "native") setOf("platform:linux", "arch:x86_64") else emptySet()),
                inputs = if (backend == "native") nativeInputs.map { BenchInput(it, role = "executable") } else emptyList())
        }
    }

    override fun run(case: BenchCase, context: BenchContext): BenchResult {
        val p = case.parameters
        val report = context.reportPath()
        val args = arguments(p["argv"])
        val error = runCatching {
            if (p.getValue("backend") == "native") {
                Files.createDirectories(report)
                runPreparedNativeCholesky(report.toFile(), args, smoke = p["smoke"]?.toBooleanStrict() ?: false,
                    qualityOnly = p.getValue("mode") == "quality")
            } else {
                val root = requireNotNull(context.worktree)
                val argv = mutableListOf("bash", root.resolve("kumulant-bench/benchmark.sh").toString(), "--prepared", "--output", report.toString())
                if (p.getValue("backend") == "scalar") argv += "--scalar"
                if (p["smoke"]?.toBooleanStrict() == true) argv += "--smoke"
                when (p.getValue("mode")) { "quality" -> argv += "--quality"; "concurrency" -> argv += "--concurrency" }
                argv += "--"; argv += args
                val exit = context.process(argv, "benchmark.log", environment = mapOf(
                    "KUMULANT_BENCH_JAVA" to Path.of(System.getProperty("java.home"), "bin", "java").toString()))
                check(exit == 0) { "Benchmark harness failed (exit $exit); inspect benchmark.log and reports" }
            }
        }.exceptionOrNull()
        val artifacts = context.artifacts() + if (Files.isRegularFile(context.outputDirectory.resolve("benchmark.log")))
            listOf(context.artifact("benchmark.log", "text/plain")) else emptyList()
        val metrics = if (error != null) emptyList() else metrics(report)
        return BenchResult(case.id, if (error == null) BenchStatus.SUCCESS else BenchStatus.FAILED,
            metrics, artifacts, message = error?.message ?: error?.javaClass?.name)
    }

    private fun metrics(report: Path): List<BenchMetric> = Files.walk(report).use { files ->
        files.filter { it.fileName.toString() == "results.json" }.sorted().flatMap { path ->
            val rows = BenchProtocol.json.parseToJsonElement(Files.readString(path)) as JsonArray
            rows.mapIndexedNotNull { index, row ->
                val metric = row.jsonObject["primaryMetric"]?.jsonObject ?: return@mapIndexedNotNull null
                val score = metric["score"]?.jsonPrimitive?.doubleOrNull?.takeIf(Double::isFinite) ?: return@mapIndexedNotNull null
                val unit = metric.getValue("scoreUnit").jsonPrimitive.content
                val mode = row.jsonObject["mode"]?.jsonPrimitive?.content
                BenchMetric("${report.relativize(path)}/$index", score, unit,
                    when (mode) { "thrpt" -> MetricDirection.HIGHER; "avgt", "ss", "sample" -> MetricDirection.LOWER; else -> MetricDirection.NONE })
            }.stream()
        }.toList()
    }
}

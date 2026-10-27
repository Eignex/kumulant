package com.eignex.kumulant.bench

import org.openjdk.jmh.runner.Runner
import org.openjdk.jmh.runner.options.CommandLineOptions
import org.openjdk.jmh.runner.options.Options
import org.openjdk.jmh.runner.options.OptionsBuilder
import org.openjdk.jmh.runner.options.TimeValue

fun main(args: Array<String>) {
    when (args.firstOrNull()) {
        "--quality" -> { analyzeQuality(); analyzeCholeskyAccuracy() }
        "--concurrency" -> analyzeConcurrency()
        "--cholesky-stress" -> analyzeCholeskyStress(args.getOrNull(1)?.toIntOrNull() ?: error("Usage: --cholesky-stress ORDER"))
        "--parameters" -> {
            println(allSpecs.joinToString(",") { it.name })
            println(allBanditSpecs.joinToString(",") { it.name })
        }
        else -> {
            val options = CommandLineOptions(*args)
            if (options.shouldHelp() || options.shouldList() || options.shouldListWithParams() || options.shouldListProfilers() || options.shouldListResultFormats()) {
                org.openjdk.jmh.Main.main(args)
            } else Runner(benchmarkOptions(options)).run()
        }
    }
}

internal fun benchmarkOptions(options: CommandLineOptions): Options {
    val smoke = java.lang.Boolean.getBoolean("bench.smoke")
    val concurrent = options.threads.orElse(1) != 1
    val names = options.getParameter("name").orElse(
        if (smoke) listOf("SumStat") else if (concurrent) listOf("SumStat", "MeanStat", "VarianceStat") else allSpecs.map { it.name },
    )
    val levels = options.getParameter("concurrency").orElse(listOf(if (concurrent) "Strict" else "None"))
    if (concurrent) {
        require(options.includes.isEmpty()) { "Concurrent timing selects shared stat updates; omit the benchmark filter" }
        require(names.all { it in listOf("SumStat", "MeanStat", "VarianceStat") } && levels.all { it in listOf("Strict", "Relaxed", "HighWrite") }) {
            "Concurrent timing requires sum/mean/variance and synchronized concurrency levels"
        }
    }
    val builder = OptionsBuilder().parent(options)
        .warmupIterations(options.warmupIterations.orElse(if (smoke) 1 else 3))
        .measurementIterations(options.measurementIterations.orElse(if (smoke) 2 else 5))
        .warmupTime(options.warmupTime.orElse(TimeValue.milliseconds(if (smoke) 100 else 1000)))
        .measurementTime(options.measurementTime.orElse(TimeValue.milliseconds(if (smoke) 100 else 1000)))
        .forks(options.forkCount.orElse(if (smoke) 1 else 2))
        .threads(options.threads.orElse(1))
        .shouldFailOnError(true)
        .param("name", *names.toTypedArray())
        .param("concurrency", *levels.toTypedArray())
        .param("policy", *options.getParameter("policy").orElse(if (smoke) listOf("MultiArmed/Thompson") else allBanditSpecs.map { it.name }).toTypedArray())
    if (options.includes.isEmpty()) builder.include(if (concurrent) ".*UpdateThroughputBenchmark.update" else ".*(UpdateThroughputBenchmark.update|BanditBenchmark.cycle)")
    return builder.build()
}

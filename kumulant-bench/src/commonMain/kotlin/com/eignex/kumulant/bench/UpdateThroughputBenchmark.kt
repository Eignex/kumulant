package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.BenchmarkMode
import kotlinx.benchmark.Mode
import kotlinx.benchmark.OutputTimeUnit
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State
import kotlinx.benchmark.TearDown
import kotlin.random.Random

/**
 * Update-loop throughput for every [StatSpec]. The `name` parameter picks the spec
 * by [StatSpec.name]; `concurrency` picks the level. The benchmark drives
 * [updatesPerInvocation] cells through `update` per invocation and JMH reports
 * operations per second — divide by [updatesPerInvocation] to get per-update cost.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(kotlinx.benchmark.BenchmarkTimeUnit.MICROSECONDS)
open class UpdateThroughputBenchmark {

    @Param(
        "SumStat",
        "CountStat",
        "TotalWeightsStat",
        "MeanStat",
        "VarianceStat",
        "MomentsStat",
        "MinStat",
        "MaxStat",
        "RangeStat",
        "BernoulliSumStat",
        "DecayingSumStat",
        "DecayingMeanStat",
        "DecayingVarianceStat",
        "EwmaMeanStat",
        "EwmaVarianceStat",
    )
    var name: String = ""

    @Param("None", "Relaxed", "Strict")
    var concurrency: String = ""

    private lateinit var stat: SeriesStat<*>
    private lateinit var values: DoubleArray
    private lateinit var weights: DoubleArray

    private val updatesPerInvocation = 4096

    @Setup
    fun setup() {
        val spec = allSpecs.first { it.name == name }
        stat = spec.factory(Concurrency.valueOf(concurrency))
        val rng = Random(1)
        values = DoubleArray(updatesPerInvocation) { rng.nextDouble() }
        weights = DoubleArray(updatesPerInvocation) { 1.0 }
    }

    @Benchmark
    fun update() {
        var i = 0
        while (i < updatesPerInvocation) {
            stat.update(values[i], 0L, weights[i])
            i++
        }
    }

    @TearDown
    fun teardown() {
        // Force a read so the JIT can't prove the snapshot is unused.
        stat.read(0L)
    }
}

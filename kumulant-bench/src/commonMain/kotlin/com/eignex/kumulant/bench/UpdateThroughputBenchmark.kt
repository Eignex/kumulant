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

/**
 * Update-loop throughput for every [StatSpec]. The `name` parameter picks the spec
 * by [StatSpec.name]; `concurrency` picks the level. Each invocation pushes
 * [updatesPerInvocation] observations through `update` and JMH reports throughput
 * — divide by [updatesPerInvocation] to recover per-update cost.
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
        "RateStat",
        "DecayingRateStat",
        "CounterRateStat",
    )
    var name: String = ""

    @Param("None", "Relaxed", "Strict")
    var concurrency: String = ""

    private lateinit var stat: SeriesStat<*>
    private lateinit var values: DoubleArray
    private lateinit var weights: DoubleArray
    private lateinit var timestamps: LongArray

    private val updatesPerInvocation = 4096

    @Setup
    fun setup() {
        val spec = allSpecs.first { it.name == name }
        stat = spec.factory(Concurrency.valueOf(concurrency))
        val workload = spec.updates(0, updatesPerInvocation).toList()
        values = DoubleArray(updatesPerInvocation) { workload[it].value }
        weights = DoubleArray(updatesPerInvocation) { workload[it].weight }
        timestamps = LongArray(updatesPerInvocation) { workload[it].timestampNanos }
    }

    @Benchmark
    fun update() {
        var i = 0
        while (i < updatesPerInvocation) {
            stat.update(values[i], timestamps[i], weights[i])
            i++
        }
    }

    @TearDown
    fun teardown() {
        stat.read(0L)
    }
}

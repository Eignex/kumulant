package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
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
 * [updatesPerInvocation] observations through the spec's `applyUpdate` and JMH
 * reports throughput — divide by [updatesPerInvocation] for per-update cost.
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
        "HyperLogLogStat",
        "LinearCountingStat",
        "BloomFilterStat",
        "CountMinSketchStat",
        "MinHashStat",
        "SpaceSavingStat",
        "DDSketchStat",
        "HdrHistogramStat",
        "LinearHistogramStat",
        "ReservoirHistogramStat",
        "TDigestStat",
        "FrugalQuantileStat",
        "UnivariateRegressionStat",
        "CovarianceStat",
        "BayesianRegressionStat",
        "DiagonalRegressionStat",
        "StochasticRegressionStat",
        "AucStat",
        "BrierScoreStat",
        "PinballLossStat",
        "ReliabilityStat",
        "DecisionTreeRegressionStat",
        "RandomForestRegressionStat",
    )
    var name: String = ""

    @Param("None", "Relaxed", "Strict")
    var concurrency: String = ""

    private lateinit var driver: BenchDriver
    private val updatesPerInvocation = 4096

    @Setup
    fun setup() {
        val spec = allSpecs.first { it.name == name }
        driver = makeDriver(spec, Concurrency.valueOf(concurrency), updatesPerInvocation)
    }

    @Benchmark
    fun update() {
        driver.runUpdates()
    }

    @TearDown
    fun teardown() {
        driver.snapshot()
    }
}

/** Type-erased driver capturing the stat instance and pre-materialised workload
 *  so the JMH hot loop avoids generic dispatch and lambda allocation. */
private class BenchDriver(
    private val applyUpdate: (Update) -> Unit,
    private val readSnapshot: () -> Unit,
    private val workload: Array<Update>,
) {
    fun runUpdates() {
        var i = 0
        while (i < workload.size) {
            applyUpdate(workload[i])
            i++
        }
    }
    fun snapshot() {
        readSnapshot()
    }
}

private fun <S, R : com.eignex.kumulant.core.Result> makeDriver(
    spec: StatSpec<S, R>,
    concurrency: Concurrency,
    n: Int,
): BenchDriver {
    val stat = spec.factory(concurrency)
    val workload = spec.updates(0, n).toList().toTypedArray()
    return BenchDriver(
        applyUpdate = { u -> spec.applyUpdate(stat, u) },
        readSnapshot = { spec.readSnapshot(stat, 0L) },
        workload = workload,
    )
}

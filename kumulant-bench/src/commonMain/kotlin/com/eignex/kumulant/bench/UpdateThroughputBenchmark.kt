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
 * reports batches per time unit; multiply by [updatesPerInvocation] for updates
 * per time unit. In average-time mode, divide batch time by [updatesPerInvocation]
 * for per-update cost.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(kotlinx.benchmark.BenchmarkTimeUnit.MICROSECONDS)
open class UpdateThroughputBenchmark {

    @Param("SumStat")
    var name: String = "SumStat"

    @Param("None")
    var concurrency: String = "None"

    private lateinit var driver: BenchDriver
    private val updatesPerInvocation = STAT_BENCHMARK_BATCH_SIZE

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
        snapshot()
    }

    internal fun snapshot(): com.eignex.kumulant.core.Result = driver.snapshot()
}

private class BenchDriver(
    private val actions: List<() -> Unit>,
    private val readSnapshot: (Long) -> com.eignex.kumulant.core.Result,
    private val workload: Array<Update>,
    private val readAt: Long,
) {
    private val stride = workload.maxOf { it.timestampNanos }
    private var snapshotAt = readAt

    fun runUpdates() {
        var i = 0
        while (i < actions.size) {
            actions[i]()
            if (stride > 0) workload[i].timestampNanos += stride
            i++
        }
        if (stride > 0) snapshotAt = workload.last().timestampNanos - stride + maxOf(0L, readAt - stride)
    }

    fun snapshot(): com.eignex.kumulant.core.Result = readSnapshot(snapshotAt)
}

private fun <S, R : com.eignex.kumulant.core.Result> makeDriver(
    spec: StatSpec<S, R>,
    concurrency: Concurrency,
    n: Int,
): BenchDriver {
    val stat = spec.factory(concurrency)
    val workload = spec.updates(STAT_BENCHMARK_SEED, n).toList().toTypedArray()
    return BenchDriver(
        actions = workload.map { update ->
            val apply = spec.prepareUpdate(update)
            val action: () -> Unit = { apply(stat) }
            action
        },
        readSnapshot = { timestamp -> spec.readSnapshot(stat, timestamp) },
        workload = workload,
        readAt = spec.readAt(n),
    )
}

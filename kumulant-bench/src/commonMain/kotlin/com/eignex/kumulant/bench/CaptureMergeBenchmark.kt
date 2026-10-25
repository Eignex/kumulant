package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Blackhole
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CaptureMergeBenchmark {
    @Param("concurrency/sum/merge/Strict/1")
    lateinit var caseId: String

    private lateinit var workload: CaptureWorkload

    @Setup
    fun setup() {
        val case = captureCases("extended").single { it.id == caseId }
        workload = mergeCaptureWorkload(allSpecs.single { it.name == case.workloadParameters.getValue("name") }, case)
    }

    @Benchmark
    fun merge(blackhole: Blackhole) = workload.consume(blackhole)
}

private fun <S, R : Result> mergeCaptureWorkload(spec: StatSpec<S, R>, case: CaptureCase): CaptureWorkload {
    val producer = spec.factory(Concurrency.None)
    spec.updates(case.seed, STAT_BENCHMARK_BATCH_SIZE).forEach { spec.applyUpdate(producer, it) }
    val snapshot = spec.readSnapshot(producer, spec.readAt(STAT_BENCHMARK_BATCH_SIZE))
    val target = spec.factory(Concurrency.valueOf(case.workloadParameters.getValue("concurrency")))
    return CaptureWorkload { spec.merge(target, snapshot); target as Any }
}

package com.eignex.kumulant.bench

import com.eignex.koblas.koblas
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import kotlin.math.abs
import kotlin.math.sqrt

internal fun analyzeQuality() {
    println("backend=${koblas.name}; stat seed=0; updates=$STAT_BENCHMARK_BATCH_SIZE")
    println("stat,actual,reference,scaled_error")
    allSpecs.forEach { statQuality(it) }
    println("bandit,mean_pseudo_regret,standard_error; seeds=0..4; rounds=512")
    for (spec in allBanditSpecs) {
        val regrets = (0..4).map { seed ->
            val workload = BanditWorkload(spec, seed)
            workload.rewards.indices.sumOf { round ->
                workload.expected[round].max() - workload.expected[round][workload.play(round)]
            }.also { check(it.isFinite() && it in 0.0..512.0) }
        }
        val mean = regrets.average()
        val error = sqrt(regrets.sumOf { (it - mean) * (it - mean) } / (regrets.size - 1) / regrets.size)
        println("${spec.name},$mean,$error")
    }
}

private fun <S, R : Result> statQuality(spec: StatSpec<S, R>) {
    val updates = spec.updates(STAT_BENCHMARK_SEED, STAT_BENCHMARK_BATCH_SIZE).toList()
    val stat = spec.factory(Concurrency.None)
    updates.forEach { spec.applyUpdate(stat, it) }
    val actual = spec.scalar(spec.readSnapshot(stat, spec.readAt(updates.size)))
    val expected = spec.reference(updates.asSequence())
    val error = abs(actual - expected) / maxOf(1.0, abs(expected))
    check(actual.isFinite() && expected.isFinite() && error.isFinite()) { "${spec.name}: nonfinite quality result" }
    if (spec.name in listOf("SumStat", "MeanStat", "VarianceStat")) check(error <= 1e-10) { "${spec.name}: error=$error" }
    println("${spec.name},$actual,$expected,$error")
}

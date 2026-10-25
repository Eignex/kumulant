package com.eignex.kumulant.bench

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Blackhole
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State
import kotlin.random.Random

@State(Scope.Benchmark)
open class BanditBenchmark {
    @Param("MultiArmed/Thompson")
    lateinit var policy: String
    private lateinit var workload: BanditWorkload

    @Setup
    fun setup() { workload = BanditWorkload(allBanditSpecs.single { it.name == policy }, 0) }

    @Benchmark
    fun cycle(blackhole: Blackhole) {
        for (round in workload.rewards.indices) blackhole.consume(workload.play(round))
    }
}

internal class BanditWorkload(spec: BanditSpec<*>, seed: Int, rounds: Int = 512) {
    private val bandit = spec.build(Random(seed))
    private val random = Random(seed xor 0xC0FFEE)
    private val contexts = List(rounds) { DenseVector.of(doubleArrayOf(random.nextDouble(-1.0, 1.0))) }
    val expected = List(rounds) { round ->
        if (spec.featureSize == 0) doubleArrayOf(0.3, 0.7)
        else if (contexts[round][0] >= 0) doubleArrayOf(1.0, 0.0) else doubleArrayOf(0.0, 1.0)
    }
    val rewards = List(rounds) { round ->
        DoubleArray(spec.nbrArms) { arm ->
            if (spec.featureSize == 0) { if (random.nextDouble() < expected[round][arm]) 1.0 else 0.0 }
            else expected[round][arm] + random.nextDouble() * 0.1
        }
    }

    fun play(round: Int): Int = when (val policy = bandit) {
        is UnivariateBandit -> policy.choose().also { policy.update(it, rewards[round][it]) }
        is ContextualBandit -> policy.choose(contexts[round]).also { policy.update(it, contexts[round], rewards[round][it]) }
        else -> error("Unsupported bandit")
    }
}

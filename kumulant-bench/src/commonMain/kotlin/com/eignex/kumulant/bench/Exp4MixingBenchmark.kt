package com.eignex.kumulant.bench

import com.eignex.koblas.core.F64DenseVector
import com.eignex.kumulant.bandit.contextual.Exp4Bandit
import com.eignex.kumulant.bandit.contextual.Exp4Expert
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class Exp4MixingBenchmark {

    @Param("8x8", "32x32", "128x128", "512x512", "8x512", "512x8")
    lateinit var shape: String

    @Param("reusable", "allocating")
    lateinit var advice: String

    private lateinit var bandit: Exp4Bandit
    private lateinit var x: F64DenseVector
    private lateinit var out: DoubleArray
    private var arm: Int = 0

    @Setup
    fun setup() {
        val (experts, arms) = shape.split('x').map(String::toInt)
        val values = Array(experts) { DoubleArray(arms) { 1.0 / arms } }
        val expertPool = values.map { value ->
            Exp4Expert { _, _ -> if (advice == "reusable") value else value.copyOf() }
        }
        bandit = Exp4Bandit(arms, expertPool, gamma = 0.1)
        x = F64DenseVector.of(doubleArrayOf(1.0))
        out = DoubleArray(arms)
    }

    @Benchmark
    fun playDistribution(): DoubleArray = bandit.playDistribution(x)

    @Benchmark
    fun playDistributionInto() {
        bandit.playDistributionInto(x, out)
    }

    @Benchmark
    fun choose(): Int = bandit.choose(x)

    @Benchmark
    fun update() {
        bandit.update(arm, x, reward = 0.0)
        arm = (arm + 1) % bandit.nbrArms
    }
}

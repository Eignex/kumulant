package com.eignex.kumulant.bench

import com.eignex.koblas.DenseVector
import com.eignex.koblas.Vector
import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.sketch.CountMinSketchStat
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Blackhole
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State
import kotlin.math.exp
import kotlin.random.Random

internal fun interface CaptureWorkload {
    fun run(): Any

    fun consume(blackhole: Blackhole) {
        blackhole.consume(run())
    }
}

@State(Scope.Benchmark)
open class CaptureWorkloadBenchmark {
    @Param("regression/update/4")
    lateinit var caseId: String

    private lateinit var workload: CaptureWorkload

    @Setup
    fun setup() {
        workload = prepareCaptureWorkload(captureCases("extended").single { it.id == caseId })
    }

    @Benchmark
    fun run(blackhole: Blackhole) = workload.consume(blackhole)
}

internal fun prepareCaptureWorkload(case: CaptureCase): CaptureWorkload = when (case.suite) {
    "regression", "storage" -> {
        val workload = RegressionCaptureWorkload(case)
        if (case.suite == "storage") {
            workload.run()
            CaptureWorkload { workload.stat.read() }
        } else workload
    }
    "bandits" -> BanditCaptureWorkload(case, case.seed)
    "sketches" -> {
        val input = sketchCaptureInput(case)
        if (case.workloadParameters.getValue("algorithm") == "tdigest") {
            val stat = TDigestStat()
            CaptureWorkload { input.forEach { stat.update(it) }; stat }
        } else {
            val stat = CountMinSketchStat(width = 128, depth = 5)
            CaptureWorkload { input.forEach { stat.update(it.toLong()) }; stat }
        }
    }
    "vectors" -> {
        val benchmark = VectorExprBenchmark().apply {
            featureSize = case.workloadParameters.getValue("featureSize").toInt()
            densityPercent = 10
            representation = case.workloadParameters.getValue("representation")
            setup()
        }
        object : CaptureWorkload {
            private fun evaluate(): Double = if (case.workloadParameters.getValue("operation") == "sum") benchmark.sum() else benchmark.dot()
            override fun run(): Double = evaluate()
            override fun consume(blackhole: Blackhole) = blackhole.consume(evaluate())
        }
    }
    else -> error("No prepared workload for ${case.suite}")
}

internal class RegressionCaptureWorkload(val case: CaptureCase) : CaptureWorkload {
    val featureSize = case.workloadParameters.getValue("featureSize").toInt()
    val input = regressionCaptureInput(featureSize, case.batchSize, case.seed)
    val stat = BayesianRegressionStat(featureSize)

    override fun run(): Any {
        for ((features, target) in input) stat.update(features, target)
        return stat
    }
}

internal fun regressionCaptureInput(featureSize: Int, count: Int, seed: Int): List<Pair<DenseVector, Double>> {
    val random = Random(seed)
    return List(count) {
        val features = DenseVector.of(DoubleArray(featureSize) { random.nextDouble(-1.0, 1.0) })
        features to regressionCaptureTarget(features)
    }
}

internal fun regressionCaptureTarget(features: Vector): Double {
    var target = 0.0
    for (i in 0 until features.size) target += (i % 7 - 3) * 0.1 * features[i]
    return target
}

internal class BanditCaptureWorkload(val case: CaptureCase, seed: Int) : CaptureWorkload {
    val spec = allBanditSpecs.single { it.name == case.workloadParameters.getValue("policy") }
    private val bandit = spec.build(Random(seed))
    private val random = Random(seed xor 0xC0FFEE)
    val contexts = List(case.batchSize) { DenseVector.of(doubleArrayOf(random.nextDouble(-1.0, 1.0))) }
    val expected = List(case.batchSize) { round ->
        if (spec.featureSize == 0) doubleArrayOf(0.3, 0.7)
        else if (contexts[round][0] >= 0.0) doubleArrayOf(1.0, 0.0) else doubleArrayOf(0.0, 1.0)
    }
    val rewards = List(case.batchSize) { round ->
        DoubleArray(spec.nbrArms) { arm ->
            if (spec.featureSize == 0) {
                if (random.nextDouble() < expected[round][arm]) 1.0 else 0.0
            } else expected[round][arm] + random.nextDouble() * 0.1
        }
    }

    fun play(round: Int): Int = when (val policy = bandit) {
        is UnivariateBandit -> policy.choose().also { policy.update(it, rewards[round][it]) }
        is ContextualBandit -> policy.choose(contexts[round]).also { policy.update(it, contexts[round], rewards[round][it]) }
        else -> error("Unsupported bandit: ${spec.name}")
    }

    private fun cycle(): Int {
        var chosen = 0
        for (round in rewards.indices) chosen = play(round)
        return chosen
    }

    override fun run(): Int = cycle()
    override fun consume(blackhole: Blackhole) = blackhole.consume(cycle())
}

internal fun sketchCaptureInput(case: CaptureCase): DoubleArray {
    val random = Random(case.seed)
    return DoubleArray(case.batchSize) {
        when (case.workloadParameters.getValue("distribution")) {
            "heavy-tail" -> exp(random.nextDouble(-4.0, 4.0))
            "skewed-keys" -> if (random.nextDouble() < 0.5) 0.0 else random.nextInt(256).toDouble()
            else -> random.nextDouble()
        }
    }
}

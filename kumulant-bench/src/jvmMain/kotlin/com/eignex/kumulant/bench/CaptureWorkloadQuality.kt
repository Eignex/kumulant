package com.eignex.kumulant.bench

import com.eignex.koblas.koblas
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.sketch.CountMinSketchStat
import com.eignex.kumulant.stat.sketch.estimate
import kotlin.math.abs
import kotlin.math.sqrt

internal fun workloadCaptureQuality(case: CaptureCase): CaptureValidation = when (case.suite) {
    "regression", "storage" -> regressionCaptureQuality(case)
    "bandits" -> banditCaptureQuality(case)
    "vectors" -> vectorCaptureQuality(case)
    "sketches" -> sketchCaptureQuality(case)
    else -> error("No quality driver for ${case.suite}")
}

private fun regressionCaptureQuality(case: CaptureCase): CaptureValidation {
    val workload = RegressionCaptureWorkload(case)
    workload.run()
    val snapshot = workload.stat.read()
    val fingerprint = CaptureFingerprint()
    for ((features, target) in workload.input) {
        features.toDoubleArray().forEach { fingerprint.add(it.toRawBits()) }
        fingerprint.add(target.toRawBits())
    }
    val heldOut = regressionCaptureInput(workload.featureSize, 128, case.seed xor 0x1234)
    var squaredError = 0.0
    var reference = 0.0
    for ((features, target) in heldOut) {
        features.toDoubleArray().forEach { fingerprint.add(it.toRawBits()) }
        fingerprint.add(target.toRawBits())
        val difference = snapshot.predict(features) - target
        squaredError += difference * difference
        reference += target * target
    }
    val metrics = mutableListOf(CaptureMetric(
        "heldOutNormalizedMse", squaredError / maxOf(1.0, reference), "ratio",
        if (case.suite == "regression") 0.02 else null,
    ))
    if (case.suite == "storage") {
        val before = snapshot.weights.toDoubleArray()
        val (features, target) = workload.input.first()
        workload.stat.update(features, target + 10.0)
        val after = snapshot.weights.toDoubleArray()
        metrics.add(CaptureMetric("snapshotMutationError", before.indices.maxOf { abs(before[it] - after[it]) }, "value", 0.0))
    }
    return CaptureValidation(koblas.name, fingerprint.finish(), metrics)
}

private fun banditCaptureQuality(case: CaptureCase): CaptureValidation {
    val fingerprint = CaptureFingerprint()
    val regrets = case.qualitySeeds.map { seed ->
        fingerprint.add(seed.toLong())
        val workload = BanditCaptureWorkload(case, seed)
        var regret = 0.0
        for (round in workload.rewards.indices) {
            workload.contexts[round].toDoubleArray().forEach { fingerprint.add(it.toRawBits()) }
            workload.rewards[round].forEach { fingerprint.add(it.toRawBits()) }
            workload.expected[round].forEach { fingerprint.add(it.toRawBits()) }
            val chosen = workload.play(round)
            regret += workload.expected[round].max() - workload.expected[round][chosen]
        }
        regret
    }
    val mean = regrets.average()
    val standardError = if (regrets.size < 2) 0.0 else sqrt(regrets.sumOf { (it - mean) * (it - mean) } / (regrets.size - 1) / regrets.size)
    val metrics = case.qualitySeeds.zip(regrets).map { (seed, regret) ->
        CaptureMetric("pseudoRegret/seed=$seed", regret, "reward", case.batchSize.toDouble())
    } + listOf(CaptureMetric("meanPseudoRegret", mean, "reward"), CaptureMetric("pseudoRegretStandardError", standardError, "reward"))
    return CaptureValidation(koblas.name, fingerprint.finish(), metrics)
}

private fun vectorCaptureQuality(case: CaptureCase): CaptureValidation {
    val size = case.workloadParameters.getValue("featureSize").toInt()
    val sparse = case.workloadParameters.getValue("representation") == "sparse"
    val count = maxOf(1, size * 10 / 100)
    val values = if (sparse) DoubleArray(size).apply { repeat(count) { this[it * size / count] = 1.0 } }
    else DoubleArray(size) { if (it % 3 == 0) 1.0 else 0.0 }
    val fingerprint = CaptureFingerprint()
    values.forEach { fingerprint.add(it.toRawBits()) }
    val expected = if (case.workloadParameters.getValue("operation") == "sum") values.sum()
    else values.indices.sumOf { values[it] * if (it % 2 == 0) 1.0 else -1.0 }
    val actual = prepareCaptureWorkload(case).run() as Double
    return CaptureValidation(koblas.name, fingerprint.finish(), listOf(CaptureMetric("absoluteError", abs(actual - expected), "value", 1e-12)))
}

private fun sketchCaptureQuality(case: CaptureCase): CaptureValidation {
    val input = sketchCaptureInput(case)
    val fingerprint = CaptureFingerprint()
    input.forEach { fingerprint.add(it.toRawBits()) }
    val metrics = if (case.workloadParameters.getValue("algorithm") == "tdigest") {
        val probabilities = doubleArrayOf(0.01, 0.5, 0.99)
        val stat = TDigestStat(probabilities = probabilities)
        input.forEach { stat.update(it) }
        val result = stat.read()
        probabilities.indices.map { index ->
            CaptureMetric("rankError/q=${probabilities[index]}", empiricalRankError(input, result.quantiles[index], probabilities[index]), "ratio", 0.05)
        }
    } else {
        val stat = CountMinSketchStat(width = 128, depth = 5)
        input.forEach { stat.update(it.toLong()) }
        val result = stat.read()
        val counts = input.map { it.toLong() }.groupingBy { it }.eachCount()
        val errors = counts.map { (key, count) -> (result.estimate(key) - count).toDouble() }
        listOf(
            CaptureMetric("maximumUnderestimate", maxOf(0.0, -errors.min()), "count", 0.0),
            CaptureMetric("meanOverestimate", errors.average(), "count"),
            CaptureMetric("maximumOverestimate", errors.max(), "count"),
        )
    }
    return CaptureValidation(koblas.name, fingerprint.finish(), metrics)
}

internal fun empiricalRankError(input: DoubleArray, estimate: Double, probability: Double): Double {
    require(input.isNotEmpty() && estimate.isFinite() && probability in 0.0..1.0)
    val lower = input.count { it < estimate }.toDouble() / input.size
    val upper = input.count { it <= estimate }.toDouble() / input.size
    return maxOf(0.0, lower - probability, probability - upper)
}

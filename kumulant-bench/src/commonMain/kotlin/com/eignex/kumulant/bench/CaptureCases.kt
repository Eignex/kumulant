package com.eignex.kumulant.bench

import kotlinx.serialization.Serializable

internal const val STAT_BENCHMARK_BATCH_SIZE = 4096
internal const val STAT_BENCHMARK_SEED = 0

@Serializable
internal data class CaptureCase(
    val id: String,
    val suite: String,
    val benchmark: String,
    val parameters: Map<String, String>,
    val seed: Int,
    val operation: String,
    val batchSize: Int = 1,
    val lifecycle: String,
    val fixtureVersion: Int = 1,
    val workloadParameters: Map<String, String> = emptyMap(),
    val qualitySeeds: List<Int> = listOf(seed),
)

internal fun captureCases(preset: String): List<CaptureCase> {
    require(preset in listOf("smoke", "standard", "extended")) { "Unknown preset: $preset" }
    val stats = listOf("SumStat", "MeanStat", "VarianceStat").map { name ->
        CaptureCase(
            id = "stats/${name.removeSuffix("Stat").lowercase()}/update",
            suite = "stats",
            benchmark = "UpdateThroughputBenchmark.update",
            parameters = mapOf("name" to name, "concurrency" to "None"),
            seed = STAT_BENCHMARK_SEED,
            operation = "update",
            batchSize = STAT_BENCHMARK_BATCH_SIZE,
            lifecycle = "Accumulate repeated weighted batches into one stat per trial; snapshot at teardown.",
        )
    }
    val matrices = when (preset) {
        "smoke" -> listOf("precision:8")
        "standard" -> listOf("precision:8", "rbf:128", "matern:129")
        else -> listOf("precision:8", "precision:128", "rbf:63", "rbf:64", "rbf:65", "rbf:128", "rbf:512", "matern:129")
    }
    val factors = matrices.map { matrix ->
        CaptureCase(
            id = "cholesky/factor/${matrix.replace(':', '/')}",
            suite = "cholesky",
            benchmark = "CholeskyFactorBenchmark.factor",
            parameters = mapOf("matrixCase" to matrix),
            seed = CholeskyFixtures.SEED,
            operation = "factorization",
            lifecycle = "Factor an unchanged matrix; validation, factor allocation, and workspace are timed.",
        )
    }
    val shapes = when (preset) {
        "smoke" -> listOf("32x1")
        "standard" -> listOf("32x1", "128x16")
        else -> listOf("32x1", "128x16", "512x64")
    }
    val solves = shapes.map { shape ->
        CaptureCase(
            id = "cholesky/solve/$shape",
            suite = "cholesky",
            benchmark = "CholeskySolveBenchmark.solveMatrix",
            parameters = mapOf("shape" to shape),
            seed = CholeskyFixtures.SEED,
            operation = "matrix solve",
            lifecycle = "Reuse one factor and unchanged right hand sides; result allocation is timed.",
        )
    }
    return stats + factors + solves + workloadCaptureCases(preset)
}

internal fun selectCaptureCases(preset: String, suite: String?, pattern: String): List<CaptureCase> {
    require(suite == null || suite in listOf("stats", "cholesky", "regression", "storage", "bandits", "vectors", "sketches")) {
        "Unknown suite: $suite"
    }
    val expression = pattern.split('*').joinToString(".*", "^", "$", transform = Regex::escape).toRegex()
    return captureCases(preset).filter { (suite == null || it.suite == suite) && expression.matches(it.id) }
        .also { require(it.isNotEmpty()) { "No cases match suite=${suite ?: "all"}, case=$pattern, preset=$preset" } }
}

private fun workloadCaptureCases(preset: String): List<CaptureCase> = buildList {
    val dimensions = when (preset) { "smoke" -> listOf(4); "standard" -> listOf(4, 16); else -> listOf(4, 16, 64) }
    for (size in dimensions) {
        for (suite in listOf("regression", "storage")) {
            val operation = if (suite == "regression") "update" else "read"
            add(workloadCaptureCase(
                "$suite/$operation/$size", suite, mapOf("featureSize" to "$size"),
                if (suite == "regression") 256 else 1,
                if (suite == "regression") "Accumulate 256 prepared feature vectors into one Bayesian stat per trial."
                else "Read an owned snapshot of a Bayesian stat trained on one prepared observation.",
            ))
        }
    }
    val policies = if (preset == "smoke") listOf(multiArmedThompsonSpec, bayesianContextualSpec) else allBanditSpecs
    for (policy in policies) add(workloadCaptureCase(
        "bandits/${policy.name}/cycle", "bandits", mapOf("policy" to policy.name), 512,
        "Choose and update for 512 prepared reward/context rounds; policy state accumulates across batches.",
    ).copy(qualitySeeds = listOf(0, 1, 2, 3, 4)))
    val representations = if (preset == "smoke") listOf("dense", "sparse") else listOf("dense", "sparse", "strided", "custom")
    for (size in dimensions) for (representation in representations) for (operation in listOf("sum", "dot")) add(workloadCaptureCase(
        "vectors/$operation/$representation/$size", "vectors",
        mapOf("featureSize" to "$size", "representation" to representation, "operation" to operation), 1,
        "Evaluate one read-only expression on a prepared $representation vector at 10 percent stored density for sparse inputs.",
    ))
    for ((algorithm, distribution) in listOf("tdigest" to "heavy-tail", "count-min" to "skewed-keys")) add(workloadCaptureCase(
        "sketches/$algorithm/$distribution", "sketches", mapOf("algorithm" to algorithm, "distribution" to distribution), 4096,
        "Accumulate 4096 prepared observations into one sketch per trial.",
    ))
}

private fun workloadCaptureCase(id: String, suite: String, parameters: Map<String, String>, batchSize: Int, lifecycle: String) = CaptureCase(
    id = id,
    suite = suite,
    benchmark = "CaptureWorkloadBenchmark.run",
    parameters = mapOf("caseId" to id),
    workloadParameters = parameters,
    seed = 0,
    operation = when (suite) {
        "bandits" -> "choose/update cycle"
        "sketches" -> "update"
        else -> id.substringAfter('/').substringBefore('/')
    },
    batchSize = batchSize,
    lifecycle = lifecycle,
)

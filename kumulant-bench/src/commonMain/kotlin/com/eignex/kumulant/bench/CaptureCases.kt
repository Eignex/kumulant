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
    return stats + factors + solves
}

internal fun selectCaptureCases(preset: String, suite: String?, pattern: String): List<CaptureCase> {
    require(suite == null || suite in listOf("stats", "cholesky")) { "Unknown suite: $suite" }
    val expression = pattern.split('*').joinToString(".*", "^", "$", transform = Regex::escape).toRegex()
    return captureCases(preset).filter { (suite == null || it.suite == suite) && expression.matches(it.id) }
        .also { require(it.isNotEmpty()) { "No cases match suite=${suite ?: "all"}, case=$pattern, preset=$preset" } }
}

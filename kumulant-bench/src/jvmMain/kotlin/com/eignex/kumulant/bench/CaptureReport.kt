package com.eignex.kumulant.bench

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

internal val captureJson = Json { prettyPrint = true; encodeDefaults = true }

@Serializable
internal data class CaptureMetric(
    val name: String,
    val value: Double,
    val unit: String,
    val maximum: Double? = null,
)

@Serializable
internal data class CaptureValidation(
    val backend: String,
    val inputSha256: String,
    val metrics: List<CaptureMetric>,
) {
    val passed: Boolean get() = metrics.isNotEmpty() && metrics.all {
        it.value.isFinite() && (it.maximum == null || it.value <= it.maximum)
    }
}

@Serializable
internal data class CaptureResult(
    val case: CaptureCase,
    val target: String,
    val status: String,
    val validation: CaptureValidation? = null,
    val metrics: List<CaptureMetric> = emptyList(),
    val raw: String? = null,
    val reason: String? = null,
)

@Serializable
internal data class CaptureResults(
    val schemaVersion: Int = 1,
    val status: String,
    val results: List<CaptureResult>,
)

internal fun writeCaptureJson(path: Path, value: String) {
    val temporary = path.resolveSibling("${path.fileName}.tmp")
    Files.writeString(temporary, value + "\n")
    Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING)
}

internal fun writeCaptureResults(output: Path, results: List<CaptureResult>, status: String) {
    val reported = captureScalingResults(results)
    writeCaptureJson(output.resolve("results.json"), captureJson.encodeToString(CaptureResults(status = status, results = reported)))
    Files.writeString(output.resolve("summary.md"), buildString {
        appendLine("# Kumulant capture")
        appendLine()
        appendLine("Status: $status. See manifest.json for settings and provenance; raw/ retains original measurements.")
        appendLine()
        appendLine("| Case | Target | Status | ns/operation | operations/s | Scaling | bytes/operation | Validation errors |")
        appendLine("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for (result in reported) {
            val timing = result.metrics.firstOrNull { it.name == "time" }?.value?.toString() ?: "—"
            val throughput = result.metrics.firstOrNull { it.name == "throughput" }?.value?.toString() ?: "—"
            val scaling = result.metrics.firstOrNull { it.name == "threadScaling" }?.value?.toString() ?: "—"
            val allocation = result.metrics.firstOrNull { it.name == "allocation" }?.value?.toString() ?: "—"
            val errors = result.validation?.metrics?.filter { it.maximum != null || it.name == "scaledDrift" }
                ?.joinToString("; ") { "${it.name}=${it.value} (${it.maximum?.let { maximum -> "max $maximum" } ?: "reported"})" } ?: "—"
            appendLine("| ${result.case.id} | ${result.target} | ${result.status} | $timing | $throughput | $scaling | $allocation | $errors |")
        }
        for (result in reported) {
            result.reason?.let { appendLine("\n${result.case.id} (${result.target}): ${it.replace('\n', ' ')}\n") }
        }
    })
}

internal fun captureScalingResults(results: List<CaptureResult>): List<CaptureResult> = results.map { result ->
    val throughput = result.metrics.firstOrNull { it.name == "throughput" }?.value
    val baseline = results.firstOrNull {
        it.target == result.target && it.case.suite == "concurrency" && it.case.threads == 1 && it.status == "passed" &&
            it.case.workloadParameters == result.case.workloadParameters
    }?.metrics?.firstOrNull { it.name == "throughput" }?.value
    if (result.case.suite != "concurrency" || result.status != "passed" || throughput == null || baseline == null) result
    else result.copy(metrics = result.metrics + listOf(
        CaptureMetric("threadScaling", throughput / baseline, "ratio"),
        CaptureMetric("parallelEfficiency", throughput / baseline / result.case.threads, "ratio"),
    ))
}

internal fun jmhMetrics(report: String, case: CaptureCase): List<CaptureMetric> {
    val rows = captureJson.parseToJsonElement(report).jsonArray
    require(rows.size == 1) { "Expected one JMH result for ${case.id}, got ${rows.size}" }
    val row = rows.single().jsonObject
    require(row.getValue("benchmark").jsonPrimitive.content == "com.eignex.kumulant.bench.${case.benchmark}") {
        "Unexpected JMH benchmark for ${case.id}"
    }
    val concurrent = "concurrency" in case.modes
    require(row.getValue("mode").jsonPrimitive.content == if (concurrent) "thrpt" else "avgt") { "Unexpected JMH measurement mode" }
    val parameters = row.getValue("params").jsonObject.mapValues { it.value.jsonPrimitive.content }
    require(parameters == case.parameters) { "Unexpected JMH parameters for ${case.id}: $parameters" }
    val primary = row.getValue("primaryMetric").jsonObject
    require(primary.getValue("scoreUnit").jsonPrimitive.content == if (concurrent) "ops/s" else "ns/op") { "Unexpected JMH score unit" }
    val score = primary.getValue("score").jsonPrimitive.double
    require(score.isFinite() && score > 0.0) { "Invalid JMH time: $score" }
    return buildList {
        add(CaptureMetric("time", if (concurrent) 1e9 / (score * case.batchSize) else score / case.batchSize, "ns/operation"))
        if (concurrent) add(CaptureMetric("throughput", score * case.batchSize, "operations/s"))
        val error = primary["scoreError"]?.jsonPrimitive?.content?.toDoubleOrNull()
        if (error != null && error.isFinite()) {
            if (concurrent) add(CaptureMetric("throughputError", error * case.batchSize, "operations/s"))
            else add(CaptureMetric("timeError", error / case.batchSize, "ns/operation"))
        }
        val secondary = row["secondaryMetrics"] as? JsonObject
        val allocation = secondary?.get("gc.alloc.rate.norm") as? JsonObject
        if (allocation != null) {
            require(allocation.getValue("scoreUnit").jsonPrimitive.content == "B/op") { "Unexpected allocation unit" }
            val bytes = allocation.getValue("score").jsonPrimitive.double
            require(bytes.isFinite() && bytes >= 0.0) { "Invalid allocation: $bytes" }
            add(CaptureMetric("allocation", bytes / case.batchSize, "bytes/operation"))
        }
    }
}

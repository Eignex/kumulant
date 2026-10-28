package com.eignex.kumulant.bench

import com.eignex.lab.bench.BenchMetric
import com.eignex.lab.bench.BenchProtocol
import com.eignex.lab.bench.MetricDirection
import java.nio.file.Files
import java.nio.file.Path
import kotlinx.serialization.json.*

internal data class LabMeasurements(val metrics: List<BenchMetric>, val details: JsonObject, val errors: List<String>, val primaryCount: Int)

internal fun labMeasurements(report: Path): LabMeasurements {
    val metrics = mutableListOf<BenchMetric>()
    val provenance = linkedMapOf<String, JsonElement>()
    val errors = mutableListOf<String>()
    var primaryCount = 0
    fun add(name: String, value: Double?, unit: String, direction: MetricDirection, source: JsonObject) {
        if (value == null || !value.isFinite()) return
        metrics += BenchMetric(name, value, unit, direction)
        provenance[name] = source
    }
    if (Files.isDirectory(report)) Files.walk(report).use { files ->
        files.filter(Files::isRegularFile).sorted().forEach { path ->
            val relative = report.relativize(path).toString().replace('\\', '/')
            runCatching {
                when (path.fileName.toString()) {
                    "results.json" -> {
                        val rows = BenchProtocol.json.parseToJsonElement(Files.readString(path)) as JsonArray
                        rows.forEachIndexed { rowIndex, element ->
                            val row = element.jsonObject
                            val mode = row["mode"]?.jsonPrimitive?.content
                            val source = buildJsonObject {
                                put("report", relative); put("row", rowIndex)
                                put("engine", if ("jmhVersion" in row) "JMH" else "kotlinx-benchmark")
                                for (key in listOf("benchmark", "mode", "params", "threads", "forks", "jvmArgs", "jdkVersion", "compilationMode")) row[key]?.let { put(key, it) }
                                val benchmark = row["benchmark"]?.jsonPrimitive?.content.orEmpty()
                                put("operationsPerInvocation", when {
                                    benchmark.endsWith("UpdateThroughputBenchmark.update") -> STAT_BENCHMARK_BATCH_SIZE
                                    benchmark.endsWith("BanditBenchmark.cycle") -> 512
                                    else -> 1
                                })
                            }
                            val primary = row["primaryMetric"]?.jsonObject ?: error("Missing primaryMetric at row $rowIndex")
                            val score = primary["score"]?.jsonPrimitive?.doubleOrNull
                            require(score != null && score.isFinite()) { "Nonfinite primary score at row $rowIndex" }
                            primaryCount++
                            val direction = when (mode) {
                                "thrpt" -> MetricDirection.HIGHER
                                "avgt", "ss", "sample" -> MetricDirection.LOWER
                                else -> MetricDirection.NONE
                            }
                            val all = listOf("primary" to primary) + row["secondaryMetrics"]?.jsonObject.orEmpty().map { it.key to it.value.jsonObject }
                            for ((label, metric) in all) {
                                val unit = metric["scoreUnit"]?.jsonPrimitive?.content ?: error("Missing scoreUnit")
                                val metricDirection = if (label == "primary") direction else when {
                                    label.startsWith("gc.alloc.rate.norm") -> MetricDirection.LOWER
                                    else -> MetricDirection.NONE
                                }
                                val name = "$relative/$rowIndex/$label"
                                add(name, metric["score"]?.jsonPrimitive?.doubleOrNull, unit, metricDirection, source)
                                add("$name/error", metric["scoreError"]?.jsonPrimitive?.doubleOrNull, unit, MetricDirection.NONE, source)
                            }
                        }
                    }
                    "quality.txt", "native-quality.txt", "concurrency.txt", "stress.txt", "native-stress.txt" -> {
                        var columns = emptyList<String>()
                        var identities = 0
                        Files.readAllLines(path).forEachIndexed { lineIndex, line ->
                            val fields = line.substringBefore(';').split(',')
                            val identityCount = when (fields.firstOrNull()) {
                                "stat" -> 1
                                "bandit" -> 1
                                "normal_sampling" -> 3
                                "fixture", "diagonal", "update" -> 3
                                "extension" -> 5
                                "retry" -> 2
                                "stress_factor" -> 3
                                "stress_sequence" -> 4
                                else -> 0
                            }
                            if (identityCount > 0 && fields.getOrNull(1) in setOf("actual", "level", "mean_pseudo_regret", "samples", "size", "kernel", "scenario", "operation")) {
                                columns = fields
                                identities = if (fields.first() == "stat" && fields.getOrNull(1) == "level") 4 else identityCount
                            } else if (fields.size == columns.size && columns.isNotEmpty()) {
                                val source = buildJsonObject { put("report", relative); put("line", lineIndex + 1); put("columns", JsonArray(columns.map(::JsonPrimitive))); put("values", JsonArray(fields.map(::JsonPrimitive))) }
                                for (index in identities until columns.size) {
                                    val column = columns[index]
                                    val value = fields[index].toDoubleOrNull() ?: fields[index].toBooleanStrictOrNull()?.let { if (it) 1.0 else 0.0 }
                                    val direction = if (column != "standard_error" && (column.contains("error") || column.contains("drift") || column.contains("reconstruction") || column.contains("regret") || column == "rmse" || column.endsWith("difference"))) MetricDirection.LOWER else MetricDirection.NONE
                                    val unit = when (column) { "snapshot_reads" -> "reads"; "count_error" -> "observations"; "mean_pseudo_regret", "standard_error" -> "reward"; "attempts" -> "attempts"; else -> "1" }
                                    add("$relative/${lineIndex + 1}/${fields.take(identities).joinToString("/")}/$column", value, unit, direction, source)
                                }
                            }
                        }
                    }
                }
            }.exceptionOrNull()?.let { errors += "$relative: ${it.message ?: it.javaClass.name}" }
        }
    }
    return LabMeasurements(metrics, buildJsonObject { put("metricSources", JsonObject(provenance)) }, errors, primaryCount)
}

package com.eignex.kumulant.bench

internal class NativeBenchmarkSettings(arguments: List<String>, smoke: Boolean) {
    private val properties = linkedMapOf(
        "filter" to ".*(UpdateThroughputBenchmark.update|BanditBenchmark.cycle)",
        "param.name" to if (smoke) "SumStat" else allSpecs.joinToString(",") { it.name },
        "param.policy" to if (smoke) "MultiArmed/Thompson" else allBanditSpecs.joinToString(",") { it.name },
        "param.concurrency" to "None",
        "warmups" to if (smoke) "1" else "3",
        "iterations" to if (smoke) "2" else "5",
        "iterationTime" to if (smoke) "100" else "1000",
        "iterationTimeUnit" to "ms",
    )

    init {
        val supplied = mutableSetOf<String>()
        for (argument in arguments) {
            require(argument.startsWith("-Pbench.") && '=' in argument && argument.none(Char::isISOControl)) {
                "Native arguments must be literal -Pbench.KEY=VALUE properties"
            }
            val key = argument.substringAfter("-Pbench.").substringBefore('=')
            val value = argument.substringAfter('=')
            require(key in properties || key in setOf("stats", "policies", "mode", "timeUnit") || key.matches(Regex("param\\.[A-Za-z][A-Za-z0-9_]*"))) {
                "Unsupported prepared Native property: $key"
            }
            require(value.isNotBlank() && supplied.add(key)) { "Blank or duplicate Native property: $key" }
            properties[key] = value
        }
        properties.remove("stats")?.let {
            require("param.name" !in supplied) { "Use stats or param.name, not both" }
            properties["param.name"] = it
        }
        properties.remove("policies")?.let {
            require("param.policy" !in supplied) { "Use policies or param.policy, not both" }
            properties["param.policy"] = it
        }
        require(properties.getValue("warmups").toIntOrNull()?.let { it > 0 } == true) { "Native warmups must be positive because the engine calibrates iterations during warmup" }
        for (key in listOf("iterations", "iterationTime")) {
            require(properties.getValue(key).toIntOrNull()?.let { it > 0 } == true) { "$key must be positive" }
        }
        require(properties.getValue("iterationTimeUnit") in setOf("ns", "us", "ms", "s", "m")) { "Unsupported Native iterationTimeUnit" }
        require(properties["mode"] == null || properties["mode"] in setOf("avgt", "thrpt")) { "Native supports avgt and thrpt" }
        require(properties["timeUnit"] == null || properties["timeUnit"] in setOf("ns", "us", "ms", "s", "m")) { "Unsupported Native timeUnit" }
        require(properties.getValue("param.concurrency") == "None") { "Native shared concurrency is unsupported" }
        require(properties.filterKeys { it.startsWith("param.") }.values.all { value -> value.split(',').none(String::isBlank) }) { "Empty Native parameter value" }
        Regex(properties.getValue("filter"))
    }

    fun arguments(): List<String> = properties.map { (key, value) -> "-Pbench.$key=$value" }

    fun configuration(): List<String> = properties.flatMap { (key, value) ->
        when {
            key == "filter" -> listOf("include:$value")
            key == "timeUnit" -> listOf("outputTimeUnit:$value")
            key.startsWith("param.") -> value.split(',').map { "param:${key.removePrefix("param.")}=$it" }
            else -> listOf("$key:$value")
        }
    }
}

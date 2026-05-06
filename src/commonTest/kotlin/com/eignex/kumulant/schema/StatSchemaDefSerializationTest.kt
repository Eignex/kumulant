package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private class HttpMetrics : StatSchema() {
    val requests by series(SumConfig)
    val latencyMs by series(DDSketchConfig(probabilities = listOf(0.5, 0.99, 0.999)))
}

private class ServiceMetrics : StatSchema() {
    val requests by series(SumConfig)
    val avgWeight by series(MeanConfig)
    val http by group(HttpMetrics())
}

class StatSchemaDefSerializationTest {

    @Test
    fun `definition encodes with @type discriminator and class names`() {
        val def = ServiceMetrics().definition()
        val encoded = SchemaJson.encodeToString(def)

        // @type must be the literal Kotlin class name; defaults must be suppressed.
        assertEquals(
            """{"stats":[""" +
                """{"name":"requests","config":{"@type":"SumConfig"}},""" +
                """{"name":"avgWeight","config":{"@type":"MeanConfig"}},""" +
                """{"name":"http","config":{"@type":"GroupStatConfig","stats":[""" +
                """{"name":"requests","config":{"@type":"SumConfig"}},""" +
                """{"name":"latencyMs","config":{"@type":"DDSketchConfig","probabilities":[0.5,0.99,0.999]}}""" +
                """]}}""" +
                """]}""",
            encoded,
        )
    }

    @Test
    fun `round-trip definition then materialize produces an equivalent live group`() {
        val original = ServiceMetrics()
        val json = SchemaJson.encodeToString(original.definition())

        val def = SchemaJson.decodeFromString<StatSchemaDef>(json)
        val specs = def.materializeSeries(Concurrency.None)
        val rebuilt = StatGroup(stats = specs)

        // Drive the same updates through both pipelines.
        original.let { schema ->
            val live = StatGroup(schema)
            live.update(1.0)
            live.update(2.0)
            live.update(3.0)

            rebuilt.update(1.0)
            rebuilt.update(2.0)
            rebuilt.update(3.0)

            val origSnap = live.read()
            val rebuiltSnap = rebuilt.read()

            assertEquals(
                origSnap[schema.requests].sum,
                (rebuiltSnap.results["requests"] as SumResult).sum,
            )
            assertEquals(
                origSnap[schema.avgWeight].mean,
                (rebuiltSnap.results["avgWeight"] as WeightedMeanResult).mean,
            )

            val origHttp = origSnap[schema.http]
            val rebuiltHttp = rebuiltSnap.results["http"] as GroupResult
            assertEquals(
                (origHttp.results["latencyMs"] as SketchResult).quantiles.toList(),
                (rebuiltHttp.results["latencyMs"] as SketchResult).quantiles.toList(),
            )
        }
    }

    @Test
    fun `definition fails loudly when entries lack a config`() {
        val mixed = object : StatSchema() {
            val good by series(SumConfig)
            val bad by series(com.eignex.kumulant.stat.summary.Sum())
        }
        val ex = assertFailsWith<IllegalArgumentException> { mixed.definition() }
        assertEquals(true, ex.message!!.contains("bad"))
    }
}

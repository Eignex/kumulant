package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.skema.SchemaJson
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
    fun `definition encodes with the discriminator and class names`() {
        val def = ServiceMetrics().statSchemaDef()
        val encoded = SchemaJson.encodeToString(def)

        assertEquals(
            """{"stats":{""" +
                """"requests":{"${'$'}type":"SumConfig"},""" +
                """"avgWeight":{"${'$'}type":"MeanConfig"},""" +
                """"http":{"${'$'}type":"GroupStatConfig","stats":{""" +
                """"requests":{"${'$'}type":"SumConfig"},""" +
                """"latencyMs":{"${'$'}type":"DDSketchConfig","probabilities":[0.5,0.99,0.999]}""" +
                """}}""" +
                """}}""",
            encoded,
        )
    }

    @Test
    fun `round-trip definition then materialize produces an equivalent live group`() {
        val schema = ServiceMetrics()
        val json = SchemaJson.encodeToString(schema.statSchemaDef())

        val def = SchemaJson.decodeFromString<StatSchemaDef>(json)
        val rebuilt = StatGroup(stats = def.materializeSeries(Concurrency.None))

        val live = StatGroup(schema)
        listOf(1.0, 2.0, 3.0).forEach { live.update(it); rebuilt.update(it) }

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

    @Test
    fun `statSchemaDef fails loudly when entries lack a config`() {
        val mixed = object : StatSchema() {
            val good by series(SumConfig)
            val bad by series(com.eignex.kumulant.stat.summary.Sum())
        }
        val ex = assertFailsWith<IllegalArgumentException> { mixed.statSchemaDef() }
        assertEquals(true, ex.message!!.contains("bad"))
    }
}

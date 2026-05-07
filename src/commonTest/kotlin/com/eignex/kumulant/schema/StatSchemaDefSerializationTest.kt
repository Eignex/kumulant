package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.skema.SchemaJson
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertEquals

private class HttpMetrics : StatSchema() {
    val requests by series(Sum)
    val latencyMs by series(DDSketch(probabilities = listOf(0.5, 0.99, 0.999)))
}

private class ServiceMetrics : StatSchema() {
    val requests by series(Sum)
    val avgWeight by series(Mean)
    val http by group(HttpMetrics())
}

class StatSchemaDefSerializationTest {

    @Test
    fun `definition encodes with the discriminator and class names`() {
        val def = ServiceMetrics().statSchemaDef()
        val encoded = SchemaJson.encodeToString(def)

        assertEquals(
            """{"stats":{""" +
                """"requests":{"${'$'}type":"Sum"},""" +
                """"avgWeight":{"${'$'}type":"Mean"},""" +
                """"http":{"${'$'}type":"GroupStatSpec","stats":{""" +
                """"requests":{"${'$'}type":"Sum"},""" +
                """"latencyMs":{"${'$'}type":"DDSketch","probabilities":[0.5,0.99,0.999]}""" +
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
        listOf(1.0, 2.0, 3.0).forEach {
            live.update(it)
            rebuilt.update(it)
        }

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
    fun `complex schema mixing modalities and operations round-trips byte-identically`() {
        val schema = object : StatSchema() {
            val requests by series(Sum)
            val weightedSum by series(Sum.withWeight(2.0))
            val count by series(Sum.withValue(1.0).withWeight(1.0))
            val ols by paired(OLS)
            val olsAtFixedX by series(OLS.withFixedX(0.5))
            val users by discrete(HyperLogLog(precision = 10))
        }

        val encoded = SchemaJson.encodeToString(schema.statSchemaDef())
        val decoded = SchemaJson.decodeFromString<StatSchemaDef>(encoded)
        val reEncoded = SchemaJson.encodeToString(decoded)

        // Round-trip is byte-identical: same field order (LinkedHashMap declaration order),
        // same defaults suppression, same discriminator placement.
        assertEquals(encoded, reEncoded)

        // Spot-check a few entries are present with the right discriminators.
        assertEquals(true, encoded.contains("\"requests\":{\"\$type\":\"Sum\"}"))
        assertEquals(true, encoded.contains("\"\$type\":\"WithWeightSeries\""))
        assertEquals(true, encoded.contains("\"\$type\":\"WithValueSeries\""))
        assertEquals(true, encoded.contains("\"\$type\":\"WithFixedX\""))
    }
}

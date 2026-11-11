@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package com.eignex.kumulant.schema

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.serialization.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.score.AccuracyResult
import com.eignex.skema.SchemaJson
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import kotlinx.serialization.protobuf.ProtoBuf
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CompositionsTest {
    @Test
    fun `named recipes retain their names and complete trees across formats`() {
        val cases = listOf(
            "recent positive" to Mean.filter(X gt 0.0).windowed(1000).named("recent positive"),
            "Mad" to Mad(200.0),
            "Covariance" to Covariance,
            "PlattCalibrator" to PlattCalibrator(),
            "Accuracy" to Accuracy(4),
            "coordinates" to Variance.vectorized(2).named("coordinates"),
            "distinct" to HyperLogLog().named("distinct"),
            "model" to StochasticRegression(featureSize = 2).named("model"),
        )

        for ((name, spec) in cases) {
            val encoded = SchemaJson.encodeToString<StatSpec>(spec)
            val body = Json.parseToJsonElement(encoded).jsonObject.getValue("named").jsonObject

            assertEquals(name, body.getValue("name").jsonPrimitive.content)
            assertTrue("inner" in body)
            assertEquals(spec, SchemaJson.decodeFromString<StatSpec>(encoded))
            assertEquals(spec, Cbor.decodeFromByteArray<StatSpec>(Cbor.encodeToByteArray<StatSpec>(spec)))
            assertEquals(spec, ProtoBuf.decodeFromByteArray<StatSpec>(ProtoBuf.encodeToByteArray<StatSpec>(spec)))
        }
    }

    @Test
    fun `naming preserves materialization through each modality root`() {
        val series = Mean.transform(X * 2.0).named("doubled")
        val paired = PairedSum.named("flows")
        val vector = Sum.vectorized(2).named("channels")
        val discrete = HyperLogLog().named("keys")
        val regression = StochasticRegression(featureSize = 1).named("fit")

        assertEquals(
            series,
            Json.decodeFromString(SeriesStatSpecSerializer, Json.encodeToString(SeriesStatSpecSerializer, series)),
        )
        assertEquals(
            paired,
            Json.decodeFromString(PairedStatSpecSerializer, Json.encodeToString(PairedStatSpecSerializer, paired)),
        )
        assertEquals(
            vector,
            Json.decodeFromString(VectorStatSpecSerializer, Json.encodeToString(VectorStatSpecSerializer, vector)),
        )
        assertEquals(
            discrete,
            Json.decodeFromString(
                DiscreteStatSpecSerializer,
                Json.encodeToString(DiscreteStatSpecSerializer, discrete),
            ),
        )
        assertEquals(
            regression,
            Json.decodeFromString(
                RegressionStatSpecSerializer,
                Json.encodeToString(RegressionStatSpecSerializer, regression),
            ),
        )
        assertEquals(6.0, series.materialize().apply { update(3.0) }.read().mean, DELTA)
        assertEquals(5.0, paired.materialize().apply { update(5.0, 8.0) }.read().sumX, DELTA)
        assertEquals(7.0, vector.materialize().apply { update(doubleArrayOf(7.0, 9.0)) }.read().results[0].sum, DELTA)
        assertTrue(discrete.materialize().apply { update(42L) }.read().estimate > 0.0)
        assertEquals(1.0, regression.materialize().apply { update(doubleArrayOf(1.0), 2.0) }.read().totalWeights)
    }

    @Test
    fun `accuracy has its own serializable result and merges weighted evidence`() {
        val source = Accuracy(3).materialize().apply {
            update(1.0, 1.0, weight = 3.0)
            update(1.0, 0.0)
        }
        val encoded = Json.encodeToString(source.read())
        val restored = Json.decodeFromString<AccuracyResult>(encoded)
        val target = Accuracy(3).materialize().apply { update(0.0, 1.0) }

        target.merge(restored)

        assertEquals(0.75, restored.accuracy, DELTA)
        assertEquals(0.6, target.read().accuracy, DELTA)
        assertEquals(5.0, target.read().totalWeights, DELTA)
        assertEquals(restored, Cbor.decodeFromByteArray<AccuracyResult>(Cbor.encodeToByteArray(restored)))
        assertEquals(restored, ProtoBuf.decodeFromByteArray<AccuracyResult>(ProtoBuf.encodeToByteArray(restored)))
    }

    @Test
    fun `projected accuracy windows preserve label admission and expire evidence`() {
        val stat = Accuracy(3).windowed(1000, slices = 2).materialize()
        stat.update(1.0, 1.0, 0L, 3.0)
        stat.update(1.0, 0.0, 500_000_000L)
        stat.update(1.5, 1.5, 600_000_000L)

        assertEquals(0.75, stat.read(600_000_000L).accuracy, DELTA)
        assertEquals(0.0, stat.read(1_100_000_000L).accuracy, DELTA)
        assertEquals(1.0, stat.read(1_100_000_000L).totalWeights, DELTA)
    }

    @Test
    fun `named mad retains center and scale through a window`() {
        val spec = Mad().named("robust").windowed(1000, slices = 2)
        val stat = spec.band(2.0).materialize()
        listOf(0.0, 10.0, 20.0).forEach { stat.update(it, 0L) }

        val result = stat.read(0L)

        assertEquals(10.0, result.center, DELTA)
        assertEquals(10.0, result.scale, DELTA)
    }

    @Test
    fun `named projections recreate independent state with the requested concurrency`() {
        val original = Covariance.materialize().apply { update(1.0, 2.0) }
        val fresh = original.create(Concurrency.Strict).apply { update(4.0, 8.0) }

        assertEquals(Concurrency.Strict, fresh.concurrency)
        assertEquals(1.0, original.read().meanX, DELTA)
        assertEquals(4.0, fresh.read().meanX, DELTA)
        fresh.reset()
        assertEquals(0.0, fresh.read().totalWeights, DELTA)
    }

    @Test
    fun `decoded projections reject incompatible inner result shapes before updates`() {
        val invalid = listOf(
            """{"project":{"inner":"sum","projection":"Mad"}}""",
            """{"project":{"inner":"pairedSum","projection":"Accuracy"}}""",
            """{"named":{"name":"broken","inner":{"asPaired":{
                "inner":{"stochasticRegression":{"featureSize":2}},
                "features":{"elements":[{"input":"x"}]}
            }}}}""",
        )
        for (json in invalid) {
            val spec = Json.decodeFromString<StatSpec>(json)
            assertFailsWith<IllegalArgumentException> { spec.materialize() }
        }
    }

    @Test
    fun `platt projection requires a one feature logit model`() {
        val incompatible = listOf(
            StochasticRegression(featureSize = 1),
            StochasticRegression(featureSize = 2, link = com.eignex.kumulant.stat.regression.glm.Link.Logit),
        )
        for (model in incompatible) {
            assertFailsWith<IllegalArgumentException> { model.asPaired(vectorOf(X)).plattCalibration().materialize() }
        }
    }

    @Test
    fun `blank composition names are rejected`() {
        assertFailsWith<IllegalArgumentException> { Mean.named(" ") }
    }

    @Test
    fun `integer predicates retain reference bindings through serialization`() {
        val predicate = Ref("label").isInteger().bind(mapOf("label" to X))
        val restored = Json.decodeFromString<BoolExpr>(Json.encodeToString(predicate))
        val cases = listOf(
            0.0 to true,
            -0.0 to true,
            2.0 to true,
            1.5 to false,
            Double.NaN to false,
            Double.POSITIVE_INFINITY to false,
        )

        for ((value, expected) in cases) assertEquals(expected, restored.eval(value))
    }
}

@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package com.eignex.kumulant.schema

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.core.IndexedResult
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.transform
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.skema.SchemaJson
import kotlinx.serialization.SerializationException
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

class ReferenceBindingsTest {
    @Test
    fun `named observations remain separate from vector coordinates`() {
        val expr = (Ref("value") + Ref("feature") + Ref("response")).bind(
            mapOf("value" to X, "feature" to V(1), "response" to Y),
        )

        assertEquals(12.0, expr.eval(2.0, 3.0, doubleArrayOf(99.0, 7.0)))
        assertEquals(12.0, expr.eval(2.0, 3.0, DenseVector.of(doubleArrayOf(99.0, 7.0))))
    }

    @Test
    fun `named feedback reads the indexed primary`() {
        val expr = (Ref("baseline") + Ref("coordinate")).bind(mapOf("baseline" to Center, "coordinate" to VIndex))
        val primary = IndexedResult(WeightedVarianceResult(totalWeights = 1.0, mean = 2.0, variance = 1.0), 3)

        assertEquals(5.0, expr.eval(100.0, primary = primary))
    }

    @Test
    fun `bindings preserve reference names when serialized`() {
        val expr = (Ref("value") + 1.0).bind(mapOf("value" to X))
        val encoded = SchemaJson.encodeToString(expr)

        assertEquals(
            """{"bind":{"expr":{"add":[{"ref":"value"},1]},"bindings":{"value":{"input":"x"}}}}""",
            encoded,
        )
        assertEquals(expr, SchemaJson.decodeFromString<ScalarExpr>(encoded))
        assertEquals(3.0, SchemaJson.decodeFromString<ScalarExpr>(encoded).eval(2.0))
    }

    @Test
    fun `boolean bindings resolve vector coordinate references`() {
        val predicate = (Ref("feature") ge 0.0).bind(mapOf("feature" to V(0)))

        assertTrue(predicate.eval(0.0, v = doubleArrayOf(4.0)))
    }

    @Test
    fun `vector bindings resolve each output coordinate`() {
        val projection = vectorOf(Ref("feature"), Ref("value")).bind(mapOf("feature" to V(0), "value" to X))

        assertEquals(listOf(4.0, 2.0), projection.eval(2.0, v = doubleArrayOf(4.0)).toList())
    }

    @Test
    fun `binding refuses unresolved references in unused branches`() {
        val expr = IfExpr(X gt 0.0, Const(1.0), Ref("missing"))

        val error = assertFailsWith<IllegalArgumentException> { expr.bind(emptyMap()) }

        assertTrue("missing" in error.message.orEmpty())
    }

    @Test
    fun `binding refuses cyclic aliases`() {
        val error = assertFailsWith<IllegalArgumentException> {
            Ref("a").bind(mapOf("a" to Ref("b"), "b" to Ref("a")))
        }

        assertTrue("Cyclic reference 'a'" in error.message.orEmpty())
    }

    @Test
    fun `nested binding scopes resolve independently`() {
        val inner = Ref("value").bind(mapOf("value" to Y))
        val expr = (Ref("value") + inner).bind(mapOf("value" to X))

        assertEquals(7.0, expr.eval(2.0, 5.0))
    }

    @Test
    fun `unbound statistical recipes fail before receiving observations`() {
        val error = assertFailsWith<IllegalArgumentException> { Sum.transform(Ref("missing")).materialize() }

        assertTrue("expr" in error.message.orEmpty())
        assertTrue("missing" in error.message.orEmpty())
    }

    @Test
    fun `decoding rejects unresolved bound references`() {
        val error = assertFailsWith<SerializationException> {
            SchemaJson.decodeFromString<ScalarExpr>("""{"bind":{"expr":{"ref":"missing"},"bindings":{}}}""")
        }

        assertTrue("missing" in error.message.orEmpty())
    }

    @Test
    fun `binding names cannot capture built in input sources`() {
        val expr = (Ref("x") + X).bind(mapOf("x" to Y))

        assertEquals(7.0, expr.eval(2.0, 5.0))
    }

    @Test
    fun `negative zero keeps its sign on the wire`() {
        val expr: ScalarExpr = Const(-0.0)
        val encoded = Json.encodeToString(expr)

        assertEquals("-0.0", encoded)
        assertEquals((-0.0).toBits(), Json.decodeFromString<ScalarExpr>(encoded).eval(0.0).toBits())
    }

    @Test
    fun `bound scalar expressions retain their sources in binary formats`() {
        val expr = (Ref("value") + 1.0).bind(mapOf("value" to X))
        val decoded = listOf(
            Cbor.decodeFromByteArray<ScalarExpr>(Cbor.encodeToByteArray(expr)),
            ProtoBuf.decodeFromByteArray<ScalarExpr>(ProtoBuf.encodeToByteArray(expr)),
        )

        for (copy in decoded) {
            assertEquals(expr, copy)
            assertEquals(3.0, copy.eval(2.0))
        }
    }

    @Test
    fun `bound vector expressions round trip with independent input sources`() {
        val expr = vectorOf(Ref("response"), Ref("coordinate"))
            .bind(mapOf("response" to Y, "coordinate" to V(1)))

        val decoded = SchemaJson.decodeFromString<VectorExpr>(SchemaJson.encodeToString(expr))

        assertEquals(expr, decoded)
        assertEquals(listOf(3.0, 7.0), decoded.eval(0.0, 3.0, doubleArrayOf(9.0, 7.0)).toList())
    }
}

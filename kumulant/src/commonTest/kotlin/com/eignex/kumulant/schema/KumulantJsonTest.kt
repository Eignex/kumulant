package com.eignex.kumulant.schema

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.transform
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.StatSpec
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.skema.SchemaJson
import com.eignex.skema.serialization.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@Serializable(with = EmbeddedStatSerializer::class)
private sealed interface EmbeddedEntry

@Serializable
private data class EmbeddedStat(val stat: StatSpec, val predicate: BoolExpr) : EmbeddedEntry

@Serializable
private data class EmbeddedModel(val entries: List<Map<String, EmbeddedEntry>>)

private val embeddedCatalog = JsonNodeCatalog { KumulantJson.serializers + EmbeddedStatSerializer }

private object EmbeddedStatSerializer : CompactSerializer<EmbeddedEntry>(
    "EmbeddedEntry",
    EmbeddedEntry::class,
    embeddedCatalog,
    { listOf(jsonNode<EmbeddedStat>("metric")) },
)

class KumulantJsonTest {
    @Test
    fun `composed catalogs decode and materialize bound statistical recipes`() {
        val projection = (Ref("value") + Ref("offset")).bind(mapOf("value" to X, "offset" to Const(1.0)))
        val predicate = (Ref("value") ge 0.0).bind(mapOf("value" to X))
        val original = EmbeddedModel(
            listOf(mapOf("observations" to EmbeddedStat(Sum.transform(projection), predicate))),
        )
        val serializer = EmbeddedModel.serializer()
        val encoded = SchemaJson.encodeToJsonElement(serializer, original)

        val decoded = SchemaJson.decodeFromJsonElement(serializer, encoded, embeddedCatalog)
        val entry = decoded.entries.single().getValue("observations") as EmbeddedStat
        val stat = entry.stat.materialize() as SeriesStat<*>
        stat.update(2.0)

        assertEquals(original, decoded)
        assertEquals(3.0, (stat.read(0L) as SumResult).sum)
        assertTrue(entry.predicate.eval(2.0))
    }

    @Test
    fun `document decoding reports collection map operator and operand paths`() {
        val json = """{"entries":[{"observations":{"metric":{"stat":"sum","predicate":{"ge":[{"ref":1},0]}}}}]}"""
        val error = assertFailsWith<SerializationException> {
            SchemaJson.decodeFromJsonElement(EmbeddedModel.serializer(), Json.parseToJsonElement(json), embeddedCatalog)
        }

        assertTrue("$.entries[0].observations.metric.predicate.ge[0].ref" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `root validation rejects unknown fields with lenient JSON settings`() {
        val json = Json { ignoreUnknownKeys = true }
        val error = assertFailsWith<SerializationException> {
            json.decodeFromJsonElement(
                EmbeddedModel.serializer(),
                Json.parseToJsonElement("""{"entries":[],"unknown":1}"""),
                embeddedCatalog,
            )
        }

        assertTrue("$.unknown" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `composed document schemas include both libraries and bound references`() {
        val schema = EmbeddedModel.serializer().toJsonSchema(embeddedCatalog)
        val definitions = schema.getValue("\$defs").jsonObject

        assertTrue("EmbeddedEntry" in definitions)
        assertTrue("com.eignex.kumulant.schema.spec.StatSpec" in definitions)
        assertTrue("com.eignex.kumulant.schema.expr.ScalarExpr" in definitions)
        assertTrue(schema.toString().contains("\"ref\""))
        assertTrue(schema.toString().contains("\"bindings\""))
        assertEquals(JsonPrimitive("https://json-schema.org/draft/2020-12/schema"), schema["\$schema"])
    }
}

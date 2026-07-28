package com.eignex.kumulant.core

import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlinx.serialization.PolymorphicSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * [IndexedResult] was the only result type in the library without `@Serializable` and
 * `@SerialName`, so it had no generated serializer and no stable wire name while every
 * sibling had both.
 *
 * Note what this does and does not establish. The annotations give it a serializer and a
 * stable discriminator, which is what this test pins. Encoding it still requires the caller
 * to supply a [SerializersModule] covering its `inner` field, because that field is typed as
 * the open [Result] interface and so resolves polymorphically. This test registers the one
 * subclass it needs; the library does not currently ship a module covering the whole result
 * catalogue.
 */
class IndexedResultSerializationTest {

    private val json = Json {
        serializersModule = SerializersModule {
            polymorphic(Result::class) {
                subclass(WeightedMeanResult::class)
                subclass(IndexedResult::class)
            }
        }
    }

    @Test
    fun `IndexedResult round trips through JSON`() {
        val original = IndexedResult(WeightedMeanResult(totalWeights = 4.0, mean = 2.5), index = 3)
        val wire = json.encodeToString(original)
        assertEquals(original, json.decodeFromString<IndexedResult>(wire))
    }

    @Test
    fun `IndexedResult carries its declared wire name`() {
        // Encoded through an explicit PolymorphicSerializer rather than `encodeToString<Result>`.
        // The reified form resolves on JVM and JS but throws on Kotlin/Native, where a
        // serializer for a non-@Serializable open interface cannot be looked up from the type
        // alone. Anything shipping results by their interface type needs the explicit form to
        // behave the same on every target.
        val wire = json.encodeToString(
            PolymorphicSerializer(Result::class),
            IndexedResult(WeightedMeanResult(1.0, 1.0), 0),
        )
        assertTrue(wire.contains("IndexedResult"), "expected the declared @SerialName in $wire")
    }

    @Test
    fun `a nested IndexedResult survives the round trip`() {
        val nested = IndexedResult(IndexedResult(WeightedMeanResult(2.0, 8.0), index = 1), index = 0)
        val wire = json.encodeToString(nested)
        assertEquals(nested, json.decodeFromString<IndexedResult>(wire))
    }
}

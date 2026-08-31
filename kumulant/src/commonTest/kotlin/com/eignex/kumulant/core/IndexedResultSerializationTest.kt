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

// Encoding an [IndexedResult] needs a caller-supplied [SerializersModule] covering its `inner` field,
// which is typed as the open [Result] interface and so resolves polymorphically. The module below
// registers the one subclass these tests need.
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
        // The reified form resolves on JVM but throws on Kotlin/Native, where a
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

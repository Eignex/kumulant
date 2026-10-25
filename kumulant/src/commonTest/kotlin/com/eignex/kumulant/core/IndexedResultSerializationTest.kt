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
        // Kotlin/Native cannot infer a serializer for the open Result interface; pass one explicitly.
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

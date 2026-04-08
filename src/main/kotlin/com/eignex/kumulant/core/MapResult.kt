package com.eignex.kumulant.core

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

@Serializable
data class ResultEntry(
    val name: String,
    val value: ResultValue
)

@Serializable
sealed interface ResultValue {
    companion object {
        fun create(value: Any): ResultValue {
            return when {
                value is Double -> ResultDouble(value)
                value is DoubleArray -> ResultArray(value)
                else -> throw IllegalArgumentException("Unsupported type for ResultValue: ${value::class.simpleName}")
            }
        }
    }
}

@Serializable
@JvmInline
value class ResultDouble(val value: Double) : ResultValue

@Serializable
@JvmInline
value class ResultArray(val values: DoubleArray) : ResultValue


object ResultValueSerializer : KSerializer<ResultEntry> {

    override fun deserialize(decoder: Decoder): ResultEntry =
        throw UnsupportedOperationException("Deserialization not supported for MapResult.")

    override val descriptor: SerialDescriptor
        get() = TODO("Not yet implemented")

    override fun serialize(
        encoder: Encoder,
        value: ResultEntry
    ) {
        TODO("Not yet implemented")
    }
}

fun Result.flatten(prefixResults: Boolean = true): List<ResultEntry> =
    flattenToList(prefixResults)

private fun Result.flattenToList(
    usePrefix: Boolean,
    prefix: String = ""
): List<ResultEntry> {
    val constructorOrder = this::class.primaryConstructor?.parameters
        ?.mapIndexed { index, param -> param.name to index }
        ?.toMap() ?: emptyMap()

    val sortedProperties = this::class.memberProperties
        .filter { it.name in constructorOrder && it.name != "name" }
        .sortedBy { constructorOrder[it.name] }

    val destination = mutableListOf<ResultEntry>()

    sortedProperties.forEach { prop ->
        val value = prop.call(this) ?: return@forEach
        val key = if (usePrefix) "$prefix${prop.name}" else prop.name

        // todo somehow result2 first/second not added
        // trying to remove trim excess a bit
        if (value is Result) {
            val name = value.nameOrDefault
            val newPrefix = if (usePrefix) "$prefix$name." else ""
            destination.addAll(value.flattenToList(usePrefix, newPrefix))
        } else {
            destination.add(ResultEntry(key, ResultValue.create(value)))
        }
    }
    return destination
}

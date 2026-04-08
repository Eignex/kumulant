package com.eignex.kumulant.core

import kotlinx.serialization.Serializable
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


fun Result.flatten(prefixResults: Boolean = true): List<ResultEntry> =
    flattenToList(prefixResults)

private fun Result.flattenToList(
    usePrefix: Boolean,
    prefix: String = ""
): List<ResultEntry> {
    // Composed result types hold sub-Results typed via generic parameters; handle them
    // directly to avoid Kotlin reflection issues with generic type erasure.
    when (this) {
        is Result2<*, *> -> {
            val dest = mutableListOf<ResultEntry>()
            listOf(first, second).forEach { r ->
                val n = r.nameOrDefault
                dest.addAll(r.flattenToList(usePrefix, if (usePrefix) "$prefix$n." else ""))
            }
            return dest
        }
        is Result3<*, *, *> -> {
            val dest = mutableListOf<ResultEntry>()
            listOf(first, second, third).forEach { r ->
                val n = r.nameOrDefault
                dest.addAll(r.flattenToList(usePrefix, if (usePrefix) "$prefix$n." else ""))
            }
            return dest
        }
        is Result4<*, *, *, *> -> {
            val dest = mutableListOf<ResultEntry>()
            listOf(first, second, third, fourth).forEach { r ->
                val n = r.nameOrDefault
                dest.addAll(r.flattenToList(usePrefix, if (usePrefix) "$prefix$n." else ""))
            }
            return dest
        }
        is ResultList<*> -> {
            val dest = mutableListOf<ResultEntry>()
            results.forEach { r ->
                val n = r.nameOrDefault
                dest.addAll(r.flattenToList(usePrefix, if (usePrefix) "$prefix$n." else ""))
            }
            return dest
        }
        else -> Unit
    }

    val constructorOrder = this::class.primaryConstructor?.parameters
        ?.mapIndexed { index, param -> param.name to index }
        ?.toMap() ?: emptyMap()

    @Suppress("UNCHECKED_CAST")
    val sortedProperties = (this::class.memberProperties as Collection<kotlin.reflect.KProperty1<Any, *>>)
        .filter { it.name in constructorOrder && it.name != "name" }
        .sortedBy { constructorOrder[it.name] }

    val destination = mutableListOf<ResultEntry>()

    sortedProperties.forEach { prop ->
        val value = prop.get(this) ?: return@forEach
        val key = if (usePrefix) "$prefix${prop.name}" else prop.name

        if (value is Result) {
            val n = value.nameOrDefault
            val newPrefix = if (usePrefix) "$prefix$n." else ""
            destination.addAll(value.flattenToList(usePrefix, newPrefix))
        } else {
            destination.add(ResultEntry(key, ResultValue.create(value)))
        }
    }
    return destination
}

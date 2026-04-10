package com.eignex.kumulant.core

import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

fun Result.flatten(prefixResults: Boolean = true): List<ResultEntry> =
    flattenToList(prefixResults)

@Suppress("UnreachableCode") // detekt false positive: type resolution infers sealed interface props, not concrete class
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

    val self: Any = this
    val constructorOrder = self::class.primaryConstructor?.parameters
        ?.mapIndexed { index, param -> param.name to index }
        ?.toMap() ?: emptyMap()

    @Suppress("UNCHECKED_CAST")
    val sortedProperties = (self::class.memberProperties as Collection<kotlin.reflect.KProperty1<Any, *>>)
        .filter { it.name in constructorOrder && it.name != "name" }
        .sortedBy { constructorOrder[it.name] }

    val destination = mutableListOf<ResultEntry>()

    for (prop in sortedProperties) {
        val value = prop.get(this) ?: continue
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

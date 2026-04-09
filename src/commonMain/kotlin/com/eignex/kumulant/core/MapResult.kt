package com.eignex.kumulant.core

import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

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

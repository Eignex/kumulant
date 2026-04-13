package com.eignex.kumulant.operation


fun interface VectorTransform {
    fun apply(vector: DoubleArray): Double
}

fun interface PairedTransform {
    fun apply(x: Double, y: Double): Double
}

fun interface DoubleTransform {
    fun apply(value: Double): Double
}

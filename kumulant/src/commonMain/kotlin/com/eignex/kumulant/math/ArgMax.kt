package com.eignex.kumulant.math

// Ties resolve to the lowest index. NaN scores lose; an all-NaN input yields index 0.
internal inline fun argMaxOf(n: Int, score: (Int) -> Double): Int {
    var bestIdx = 0
    var bestScore = Double.NEGATIVE_INFINITY
    for (i in 0 until n) {
        val s = score(i)
        if (s > bestScore) {
            bestScore = s
            bestIdx = i
        }
    }
    return bestIdx
}

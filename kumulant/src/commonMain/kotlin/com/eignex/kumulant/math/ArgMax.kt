package com.eignex.kumulant.math

/**
 * Index of the largest of [n] scores, resolving ties to the lowest index.
 *
 * A NaN score always loses, because the running best starts at `-Infinity` and every comparison against
 * NaN is false. An all-NaN set therefore yields index 0.
 */
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

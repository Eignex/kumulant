package com.eignex.kumulant.math

/**
 * Index of the largest of [n] scores, resolving ties to the lowest index.
 *
 * Eight sites open-coded this: four bandits over arms, and four models over class labels. They were not
 * quite the same loop, and the difference was in NaN handling.
 *
 * The bandits seeded `best` at `-Infinity`; the models seeded it at `score(0)`. Since every comparison
 * against NaN is false, seeding from `score(0)` means a NaN *first* score wins outright - nothing can
 * ever beat it - so one poisoned class label decided every later prediction. Seeding at `-Infinity`
 * makes a NaN score lose instead, and only an all-NaN set falls through to index 0.
 *
 * This unifies on `-Infinity`, which changes what a poisoned model predicts: previously whichever class
 * happened to go NaN first, now the best of the classes that are still finite. Both are permitted - a
 * non-finite value is allowed to poison a stat and only guaranteed not to throw - but a model that keeps
 * answering from its surviving coordinates is the more useful of the two, and having one convention
 * rather than two is the point.
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

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.HasObservationCount
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

// Breiman random-subspace size: ceil(sqrt(p)); an empty candidate pool yields zero.
internal fun defaultMtry(p: Int): Int = if (p <= 0) 0 else ceil(sqrt(p.toDouble())).toInt().coerceAtLeast(1)

// VFDT margin with confidence delta * decay^depth; empty leaves have an infinite bound.
internal fun hoeffdingBound(delta: Double, n: Double, depth: Int, decay: Double): Double {
    if (n <= 0.0) return Double.POSITIVE_INFINITY
    val adjusted = delta * decay.pow(depth.toDouble())
    return sqrt(-ln(adjusted) / (2.0 * n))
}

internal fun <S> List<S>.pickCandidates(mtry: Int?, random: Random): List<S> {
    val k = mtry ?: return this
    if (k >= size) return this
    return shuffled(random).take(k)
}

internal fun <R : HasObservationCount> rankCandidates(
    total: R,
    pos: List<R>,
    neg: List<R>,
    minSamplesSplit: Double,
    minSamplesLeaf: Double,
    score: (R, R, R) -> Double,
): SplitInfo {
    require(pos.size == neg.size) { "pos and neg lists must align: ${pos.size} vs ${neg.size}" }
    var top1 = 0.0
    var top2 = 0.0
    var bestI = -1
    for (i in pos.indices) {
        val wPos = pos[i].totalWeights
        val wNeg = neg[i].totalWeights
        if (wPos < minSamplesLeaf || wNeg < minSamplesLeaf || wPos + wNeg < minSamplesSplit) continue
        val v = score(total, pos[i], neg[i])
        when {
            v > top1 -> {
                top2 = top1
                top1 = v
                bestI = i
            }

            v > top2 -> top2 = v
        }
    }
    return SplitInfo(top1, top2, bestI)
}

// The Hoeffding margin establishes candidate ordering; tau permits ties once the bound is small enough.
internal fun shouldSplit(ranked: SplitInfo, totalWeight: Double, depth: Int, config: HoeffdingTreeConfig): Boolean {
    if (totalWeight < config.minSamplesSplit) return false
    if (ranked.bestIndex < 0 || ranked.top1 <= 0.0) return false
    val eps = hoeffdingBound(config.delta, totalWeight, depth, config.deltaDecay)
    return ranked.top1 - ranked.top2 > eps || eps < config.tau
}

// Normalizing by parent impurity puts every criterion in [0, 1], the range assumed by the Hoeffding bound.
internal inline fun <R : HasObservationCount> impurityReduction(
    total: R,
    pos: R,
    neg: R,
    impurity: (R) -> Double,
): Double {
    val wPos = pos.totalWeights
    val wNeg = neg.totalWeights
    val w = wPos + wNeg
    if (w <= 0.0) return 0.0
    val parent = impurity(total)
    // An already-pure parent has nothing to remove, so no candidate improves on not splitting.
    if (parent <= 0.0) return 0.0
    val weighted = (wPos / w) * impurity(pos) + (wNeg / w) * impurity(neg)
    return (parent - weighted) / parent
}

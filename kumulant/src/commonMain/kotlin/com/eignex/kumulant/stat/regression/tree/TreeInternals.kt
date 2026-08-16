package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.HasObservationCount
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Breiman's default random-subspace size: `ceil(sqrt(p))` candidates drawn per audit leaf.
 *
 * Both forests default `mtry` to this when the caller leaves it null, and both used to carry their own
 * private copy of the expression. The clamp matters at the small end - `ceil(sqrt(1))` is already 1, but
 * `coerceAtLeast(1)` keeps an mtry of zero from disabling growth silently if the arithmetic is ever
 * changed - and `p <= 0` returns 0 rather than 1 because an empty candidate pool means no growth at all
 * rather than growth on one candidate that does not exist.
 *
 * @param p size of the tree's full candidate-split pool.
 */
internal fun defaultMtry(p: Int): Int = if (p <= 0) 0 else ceil(sqrt(p.toDouble())).toInt().coerceAtLeast(1)

/**
 * The VFDT split margin: how far ahead a candidate must be before the tree believes the ordering.
 *
 * Hoeffding's inequality bounds how far a mean over [n] observations can sit from the true mean at
 * confidence [delta], and a split fires when the best candidate beats the runner-up by more than this.
 * That is what makes the tree's decisions stable rather than a reaction to whichever candidate happened
 * to look good first.
 *
 * [delta] is decayed by [decay] raised to [depth] before use: a deeper leaf sees less of the stream, so
 * holding confidence fixed would let it split on proportionally thinner evidence. An empty leaf returns
 * an infinite bound, so no margin can ever clear it and the leaf cannot split on no data.
 *
 * Both trees carried a byte-identical private copy of this.
 *
 * @param delta confidence threshold before depth decay.
 * @param n total observation weight at the leaf.
 * @param depth the leaf's depth, driving the decay.
 * @param decay per-depth multiplier on [delta].
 */
internal fun hoeffdingBound(delta: Double, n: Double, depth: Int, decay: Double): Double {
    if (n <= 0.0) return Double.POSITIVE_INFINITY
    val adjusted = delta * decay.pow(depth.toDouble())
    return sqrt(-ln(adjusted) / (2.0 * n))
}

/**
 * Draw the random subspace of candidate splits a newly born audit leaf will consider.
 *
 * With [mtry] null, or at least as large as the pool, the leaf considers everything and the list is
 * returned as is rather than copied. Otherwise it gets a fresh random subset, which is what decorrelates
 * the trees in a forest: two trees seeing the same stream still grow differently because they were
 * offered different candidates.
 *
 * Generic in the split type because the regression tree is generic in its row type while the
 * classification tree is fixed to `VectorView`; both trees had their own copy of this.
 *
 * @param S the split type, which differs between the two trees.
 * @param mtry subspace size, or null for the full pool.
 * @param random the tree's PRNG, so a seeded tree stays reproducible.
 */
internal fun <S> List<S>.pickCandidates(mtry: Int?, random: Random): List<S> {
    val k = mtry ?: return this
    if (k >= size) return this
    return shuffled(random).take(k)
}

/**
 * Score every candidate split at a leaf and return the winner, the runner-up, and the winner's index.
 *
 * Generic in the leaf payload, which is the only thing that differed between the regression and
 * classification copies of this loop. Both needed `totalWeights` and a scoring function and nothing
 * else, so both are supplied here rather than reached through a payload-specific metric type.
 *
 * A candidate is skipped, not scored, when either side is thinner than [minSamplesLeaf] or the two
 * sides together are thinner than [minSamplesSplit]: a split that isolates three observations may score
 * beautifully and predict nothing. Skipped candidates cannot become `bestIndex`, so a leaf where every
 * candidate is too thin reports `bestIndex = -1` and does not split.
 *
 * The runner-up is tracked because that, not the winner's absolute score, is what the Hoeffding test
 * consumes: the question is whether the tree is confident about the *ordering*, so a leaf with two
 * equally good candidates has to wait even though both are excellent.
 *
 * @param R the leaf payload type.
 * @param total the leaf's pre-split aggregate.
 * @param pos per-candidate aggregate of observations routing true.
 * @param neg per-candidate aggregate of observations routing false.
 * @param minSamplesSplit minimum combined weight for a candidate to be scored.
 * @param minSamplesLeaf minimum weight on each side for a candidate to be scored.
 * @param score the split criterion, taking total / pos / neg.
 */
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

/**
 * The VFDT split decision: given ranked candidates, has the leaf earned the right to split?
 *
 * Three gates, and all three used to be written out twice with nothing keeping them in step.
 *
 * A candidate must exist and be worth taking: `bestIndex < 0` means every candidate was too thin to
 * score, and `top1 <= 0.0` means the best one does not improve on not splitting at all, since every
 * metric is defined so that no signal scores zero.
 *
 * Then either the Hoeffding test or the tie-break has to pass. The Hoeffding test is the real rule: the
 * winner must beat the runner-up by more than the bound, so the ordering is not an artefact of which
 * candidate happened to look good first. The tie-break exists because that rule alone deadlocks - two
 * candidates of genuinely equal merit never separate by more than any bound, so the leaf would grow
 * forever without splitting. Once the bound itself has shrunk below [HoeffdingTreeConfig.tau] the tree
 * has enough evidence to know the choice does not matter much, and picks the winner anyway.
 *
 * @param ranked output of [rankCandidates].
 * @param totalWeight the leaf's accumulated observation weight, which drives the bound.
 * @param depth the leaf's depth, which decays the confidence level.
 * @param config the growth tunables.
 */
internal fun shouldSplit(ranked: SplitInfo, totalWeight: Double, depth: Int, config: HoeffdingTreeConfig): Boolean {
    if (totalWeight < config.minSamplesSplit) return false
    if (ranked.bestIndex < 0 || ranked.top1 <= 0.0) return false
    val eps = hoeffdingBound(config.delta, totalWeight, depth, config.deltaDecay)
    return ranked.top1 - ranked.top2 > eps || eps < config.tau
}

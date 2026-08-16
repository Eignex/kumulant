package com.eignex.kumulant.stat.regression.tree

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

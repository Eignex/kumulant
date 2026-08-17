package com.eignex.kumulant.core

import com.eignex.koblas.VectorView

/**
 * Reject a context vector whose arity does not match the model's.
 *
 * Every [RegressionStat] opens its `update` with this check, and every linear-model [Result] repeats
 * it in `predict` / `logit`, because a mismatched vector does not fail loudly on its own: a *short*
 * vector simply reads fewer coordinates and returns a plausible number from the wrong model.
 *
 * The message is fixed at `x.size=<n>, expected <m>` so it stays greppable. `HalfSpaceTreesStat.update`
 * spells its own check out instead, because its receiver is named `vector` and its message says so.
 *
 * @param expected the model's feature count.
 * @throws IllegalArgumentException if [VectorView.size] differs from [expected].
 */
@Suppress("NOTHING_TO_INLINE") // as with the weight predicates; the non-JVM targets pay for the call
internal inline fun VectorView.requireFeatureSize(expected: Int) {
    require(size == expected) { "x.size=$size, expected $expected" }
}

/** Reject a non-positive feature count at construction. */
internal fun requirePositiveFeatureSize(featureSize: Int) {
    require(featureSize > 0) { "featureSize must be positive, got $featureSize" }
}

/** Reject a class count below two: one class is not a classification problem. */
internal fun requireAtLeastTwoClasses(numClasses: Int) {
    require(numClasses >= 2) { "numClasses must be >= 2; got $numClasses" }
}

/** Reject a non-positive bin count. */
internal fun requirePositiveBins(numBins: Int) {
    require(numBins > 0) { "numBins must be > 0; got $numBins" }
}

/**
 * Reject a merge between two models of different arity.
 *
 * Distinct from [requireFeatureSize], which guards an incoming observation: this guards an incoming
 * *snapshot*, where a mismatch means the two were never the same model.
 */
internal fun requireMergeFeatureSize(incoming: Int, own: Int) {
    require(incoming == own) { "merge: featureSize mismatch $incoming vs $own" }
}

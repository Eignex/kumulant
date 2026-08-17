package com.eignex.kumulant.core

import com.eignex.koblas.VectorView

/**
 * Reject a context vector whose arity does not match the model's.
 *
 * Every [RegressionStat] opens its `update` with this check, and every linear-model [Result] repeats
 * it in `predict` / `logit`, because a mismatched vector does not fail loudly on its own: a *short*
 * vector simply reads fewer coordinates and returns a plausible number from the wrong model. Fourteen
 * sites spelled the same `require` out by hand, twice within the same file in two of them.
 *
 * The message is deliberately identical to the hand-written form - `x.size=<n>, expected <m>` - so it
 * stays greppable and so the assertion in `RegressionOpsTest` keeps meaning what it meant. The one
 * site left spelling it out is `HalfSpaceTreesStat.update`, whose receiver is named `vector` and whose
 * message says so.
 *
 * `VectorizedStat.update` was a second such site, with a third wording again, until it was migrated
 * here - so treat the count above as something to re-check rather than trust.
 *
 * @param expected the model's feature count.
 * @throws IllegalArgumentException if [VectorView.size] differs from [expected].
 */
@Suppress("NOTHING_TO_INLINE") // as with the weight predicates; the non-JVM targets pay for the call
internal inline fun VectorView.requireFeatureSize(expected: Int) {
    require(size == expected) { "x.size=$size, expected $expected" }
}

/**
 * Reject a non-positive feature count at construction.
 *
 * Sixteen sites spelled this out, in *two* spellings: the GLM stats said `"featureSize must be positive"`
 * and the tree stats appended `", got $featureSize"`. Standardised on the with-value form, since a
 * message naming the offending number is strictly more useful and no test matched either one.
 */
internal fun requirePositiveFeatureSize(featureSize: Int) {
    require(featureSize > 0) { "featureSize must be positive, got $featureSize" }
}

/** Reject a class count below two. One class is not a classification problem. Nine identical sites. */
internal fun requireAtLeastTwoClasses(numClasses: Int) {
    require(numClasses >= 2) { "numClasses must be >= 2; got $numClasses" }
}

/** Reject a non-positive bin count. Six identical sites across the score and calibration families. */
internal fun requirePositiveBins(numBins: Int) {
    require(numBins > 0) { "numBins must be > 0; got $numBins" }
}

/**
 * Reject a merge between two models of different arity.
 *
 * Distinct from [requireFeatureSize], which guards an incoming observation: this guards an incoming
 * *snapshot*, where the mismatch means the two models were never the same model. Three identical sites.
 */
internal fun requireMergeFeatureSize(incoming: Int, own: Int) {
    require(incoming == own) { "merge: featureSize mismatch $incoming vs $own" }
}

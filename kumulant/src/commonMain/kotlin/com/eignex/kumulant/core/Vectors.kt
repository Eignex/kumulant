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
 * @param expected the model's feature count.
 * @throws IllegalArgumentException if [VectorView.size] differs from [expected].
 */
@Suppress("NOTHING_TO_INLINE") // as with the weight predicates; the non-JVM targets pay for the call
internal inline fun VectorView.requireFeatureSize(expected: Int) {
    require(size == expected) { "x.size=$size, expected $expected" }
}

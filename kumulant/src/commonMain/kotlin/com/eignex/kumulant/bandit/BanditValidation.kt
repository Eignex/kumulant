package com.eignex.kumulant.bandit

/**
 * The three preconditions every bandit shares, each stated once.
 *
 * They were spelled out inline instead: the arm-count message at eight sites, the bounds message at
 * eight, and the merge-arity message at seven. That is not merely repetitive - the bounds check was
 * *missing* from [com.eignex.kumulant.bandit.contextual.RegressionContextualBandit] entirely, on all
 * four of its arm-indexed entry points, while a comment in `MultiArmedBandit.update` asserted that
 * "every sibling bandit validates this". Twenty-three hand-copied `require` calls are exactly the
 * shape in which one of them goes missing and the comments keep claiming otherwise.
 *
 * The messages are preserved byte for byte, since they are what the tests match on.
 */

/** Guards the constructor: a bandit with no arms has nothing to choose between. */
internal fun requireNbrArms(nbrArms: Int) {
    require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
}

/**
 * Guards every entry point that takes an arm index.
 *
 * Without it a bad index surfaces as a raw `IndexOutOfBoundsException` from whatever array happens to
 * be indexed first, rather than the `IllegalArgumentException` the interfaces document.
 */
internal fun requireArmIndex(armIndex: Int, nbrArms: Int) {
    require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
}

/** Guards a merge: two bandits over different arm counts have no correspondence to merge along. */
internal fun requireMergeSize(size: Int, nbrArms: Int) {
    require(size == nbrArms) { "merge: other.size=$size does not match nbrArms=$nbrArms" }
}

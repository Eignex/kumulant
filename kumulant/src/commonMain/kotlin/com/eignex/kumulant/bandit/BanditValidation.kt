package com.eignex.kumulant.bandit

/*
 * The preconditions every bandit shares. Every arm-indexed entry point on every bandit is expected to
 * call requireArmIndex; ArmIndexContractSweepTest enforces that across the family.
 */

/** Guards the constructor: a bandit with no arms has nothing to choose between. */
internal fun requireNbrArms(nbrArms: Int) {
    require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
}

/**
 * Guards every entry point that takes an arm index, so a bad index raises the
 * `IllegalArgumentException` the interfaces document rather than a raw `IndexOutOfBoundsException`.
 */
internal fun requireArmIndex(armIndex: Int, nbrArms: Int) {
    require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
}

/** Guards a merge: two bandits over different arm counts have no correspondence to merge along. */
internal fun requireMergeSize(size: Int, nbrArms: Int) {
    require(size == nbrArms) { "merge: other.size=$size does not match nbrArms=$nbrArms" }
}

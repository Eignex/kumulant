package com.eignex.kumulant.bandit

/*
 * The preconditions every bandit shares. Every arm-indexed entry point on every bandit is expected to
 * call requireArmIndex; ArmIndexContractSweepTest enforces that across the family.
 */

internal fun requireNbrArms(nbrArms: Int) {
    require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
}

internal fun requireArmIndex(armIndex: Int, nbrArms: Int) {
    require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
}

internal fun requireMergeSize(size: Int, nbrArms: Int) {
    require(size == nbrArms) { "merge: other.size=$size does not match nbrArms=$nbrArms" }
}

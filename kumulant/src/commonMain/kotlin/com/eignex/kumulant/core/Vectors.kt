package com.eignex.kumulant.core

import com.eignex.koblas.Vector
import com.eignex.koblas.forEachStored

// A short vector can produce a plausible prediction from only part of the model.
// Avoid call overhead on targets without JIT inlining.
@Suppress("NOTHING_TO_INLINE")
internal inline fun Vector.requireFeatureSize(expected: Int) {
    require(size == expected) { "x.size=$size, expected $expected" }
}

internal fun Vector.hasNonFiniteStored(): Boolean {
    var nonFinite = false
    forEachStored { _, value -> if (!value.isFinite()) nonFinite = true }
    return nonFinite
}

internal fun requirePositiveFeatureSize(featureSize: Int) {
    require(featureSize > 0) { "featureSize must be positive, got $featureSize" }
}

internal fun requireAtLeastTwoClasses(numClasses: Int) {
    require(numClasses >= 2) { "numClasses must be >= 2; got $numClasses" }
}

internal fun requirePositiveBins(numBins: Int) {
    require(numBins > 0) { "numBins must be > 0; got $numBins" }
}

internal fun requireMergeFeatureSize(incoming: Int, own: Int) {
    require(incoming == own) { "merge: featureSize mismatch $incoming vs $own" }
}

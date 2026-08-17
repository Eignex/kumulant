package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.preview
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.pow

/** Single estimated quantile with the [probability] it targets. */
@Serializable
@SerialName("QuantileResult")
data class QuantileResult(
    /** Probability the [quantile] targets. */
    val probability: Double,
    /** Estimated quantile value at [probability]. */
    val quantile: Double,
) : Result

/** DDSketch snapshot: logarithmic bins plus precomputed [quantiles] for [probabilities]. */
@Serializable
@SerialName("SketchResult")
data class SketchResult(
    /** Probabilities at which [quantiles] are evaluated; parallel to [quantiles]. */
    val probabilities: DoubleArray,
    /** Estimated quantile values, parallel to [probabilities]. */
    val quantiles: DoubleArray,
    /** Multiplicative bin-boundary ratio `(1 + relativeError) / (1 - relativeError)`. */
    val gamma: Double,
    /**
     * Cumulative observation weight folded in, equal to [zeroCount] plus the sum of
     * [positiveBins] and [negativeBins]. Reported for convenience, not tracked
     * independently: merge folds in the bins and the zero bucket and recomputes this, so a
     * hand-built result whose total disagrees with its bins contributes only its bins.
     */
    override val totalWeights: Double,
    /** Weight observed at the zero bin. */
    val zeroCount: Double,
    /** Positive-side bin counts keyed by signed log-bucket index. */
    val positiveBins: Map<Int, Double>,
    /** Negative-side bin counts keyed by signed log-bucket index. */
    val negativeBins: Map<Int, Double>,
) : HasObservationCount {
    override fun equals(other: Any?): Boolean = other is SketchResult &&
        probabilities.contentEquals(other.probabilities) &&
        quantiles.contentEquals(other.quantiles) &&
        gamma == other.gamma &&
        totalWeights == other.totalWeights &&
        zeroCount == other.zeroCount &&
        positiveBins == other.positiveBins &&
        negativeBins == other.negativeBins

    override fun hashCode(): Int {
        var h = probabilities.contentHashCode()
        h = 31 * h + quantiles.contentHashCode()
        h = 31 * h + gamma.hashCode()
        h = 31 * h + totalWeights.hashCode()
        h = 31 * h + zeroCount.hashCode()
        h = 31 * h + positiveBins.hashCode()
        h = 31 * h + negativeBins.hashCode()
        return h
    }

    override fun toString(): String = "SketchResult(" +
        "probabilities=${probabilities.preview()}, " +
        "quantiles=${quantiles.preview()}, " +
        "gamma=$gamma, " +
        "totalWeights=$totalWeights, " +
        "zeroCount=$zeroCount, " +
        "positiveBins=$positiveBins, " +
        "negativeBins=$negativeBins)"
}

/** Histogram as parallel `[lowerBounds, upperBounds)` bucket arrays with [weights]. */
@Serializable
@SerialName("SparseHistogramResult")
data class SparseHistogramResult(
    /** Inclusive lower bound of each bucket; parallel to [upperBounds] and [weights]. */
    val lowerBounds: DoubleArray,
    /** Exclusive upper bound of each bucket; parallel to [lowerBounds] and [weights]. */
    val upperBounds: DoubleArray,
    /** Observed weight per bucket; parallel to [lowerBounds] / [upperBounds]. */
    val weights: DoubleArray,
) : Result {
    override fun equals(other: Any?): Boolean = other is SparseHistogramResult &&
        lowerBounds.contentEquals(other.lowerBounds) &&
        upperBounds.contentEquals(other.upperBounds) &&
        weights.contentEquals(other.weights)

    override fun hashCode(): Int {
        var h = lowerBounds.contentHashCode()
        h = 31 * h + upperBounds.contentHashCode()
        h = 31 * h + weights.contentHashCode()
        return h
    }

    override fun toString(): String = "SparseHistogramResult(" +
        "lowerBounds=${lowerBounds.preview()}, " +
        "upperBounds=${upperBounds.preview()}, " +
        "weights=${weights.preview()})"
}

/** Project a [SketchResult] into a [SparseHistogramResult] by expanding its bin indices to bucket boundaries. */
fun SketchResult.toSparseHistogram(): SparseHistogramResult {
    val hasZero = zeroCount > 0.0
    val totalBuckets = negativeBins.size + (if (hasZero) 1 else 0) + positiveBins.size

    val lowers = DoubleArray(totalBuckets)
    val uppers = DoubleArray(totalBuckets)
    val weights = DoubleArray(totalBuckets)

    var cursor = 0

    // Negative bins: most-negative first (descending index) so the output remains ordered low-to-high.
    negativeBins.entries.sortedByDescending { it.key }.forEach { (index, weight) ->
        lowers[cursor] = -(gamma.pow(index))
        uppers[cursor] = -(gamma.pow(index - 1))
        weights[cursor] = weight
        cursor++
    }

    if (hasZero) {
        lowers[cursor] = 0.0
        uppers[cursor] = 0.0
        weights[cursor] = zeroCount
        cursor++
    }

    positiveBins.entries.sortedBy { it.key }.forEach { (index, weight) ->
        lowers[cursor] = gamma.pow(index - 1)
        uppers[cursor] = gamma.pow(index)
        weights[cursor] = weight
        cursor++
    }

    return SparseHistogramResult(
        lowerBounds = lowers,
        upperBounds = uppers,
        weights = weights,
    )
}

/**
 * How close two sketch parameters must be before a merge is allowed.
 *
 * [DDSketchStat] compares relative-error targets and [TDigestStat] compares compressions, both against a
 * bare `1e-9`. The comparison is approximate rather than exact because these are `Double`s that may have
 * round-tripped through a wire format, so a sketch merged with a re-decoded copy of itself must still be
 * accepted. It is not a numerical tolerance on the *estimate* - a genuinely different parameter means
 * incompatible bucket layouts and no error bound at all, so anything past this is refused outright.
 *
 * Named because `1e-9` appears in this package with three unrelated meanings: this, the bin-alignment
 * check in [LinearHistogramStat], and [DDSketchStat]'s default `minIndexableValue`.
 */
internal const val PARAMETER_MATCH_TOLERANCE: Double = 1e-9

/**
 * How closely an incoming bucket edge must land on a bin boundary to be merged into that bin.
 *
 * Distinct from [PARAMETER_MATCH_TOLERANCE] despite the same value: this one is a tolerance on where a
 * bucket edge sits after division, not on whether two configurations match.
 */
internal const val BIN_ALIGNMENT_TOLERANCE: Double = 1e-9

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireAtLeastTwoClasses
import com.eignex.kumulant.math.argMaxOf
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ln

/**
 * Snapshot of a K-bin weighted class-count vector. Used as the leaf aggregate for
 * [ClassificationTree]: the per-leaf running tally of how many (weighted) observations
 * of each class landed in this leaf. Exposes derived class probabilities and the
 * argmax prediction.
 */
@Serializable
@SerialName("ClassCountsResult")
data class ClassCountsResult(
    /** Number of classes. */
    val numClasses: Int,
    /** Per-class accumulated weight; length [numClasses]. */
    val counts: DoubleArray,
) : HasObservationCount {

    init {
        requireAtLeastTwoClasses(numClasses)
        require(counts.size == numClasses) { "counts must have length $numClasses; got ${counts.size}" }
    }

    /** Total weight summed across all classes. */
    override val totalWeights: Double get() {
        var s = 0.0
        for (c in counts) s += c
        return s
    }

    /** Class probabilities; uniform when no observations have been seen yet. */
    fun probabilities(): DoubleArray {
        val w = totalWeights
        if (w <= 0.0) return DoubleArray(numClasses) { 1.0 / numClasses }
        return DoubleArray(numClasses) { counts[it] / w }
    }

    /** Argmax class index; ties resolve to the lowest index. */
    fun predict(): Int = argMaxOf(numClasses) { k -> counts[k] }

    /** Shannon entropy of the empirical class distribution, in nats. Zero on an empty leaf. */
    val entropy: Double get() {
        val w = totalWeights
        if (w <= 0.0) return 0.0
        var h = 0.0
        for (c in counts) {
            if (c > 0.0) {
                val p = c / w
                h -= p * ln(p)
            }
        }
        return h
    }

    /** Gini impurity `1 - Sum p_k^2`. Zero on a pure leaf or an empty leaf. */
    val gini: Double get() {
        val w = totalWeights
        if (w <= 0.0) return 0.0
        var s = 0.0
        for (c in counts) {
            val p = c / w
            s += p * p
        }
        return 1.0 - s
    }

    override fun equals(other: Any?): Boolean = other is ClassCountsResult &&
        numClasses == other.numClasses && counts.contentEquals(other.counts)

    override fun hashCode(): Int = 31 * numClasses + counts.contentHashCode()
}

/**
 * Series stat over discrete class-index inputs; the snapshot is a weighted
 * per-class count vector. `update(value, weight)` resolves `value` to a class
 * index via [asClassLabel], so only a `Double` that is exactly an integer in
 * `[0, numClasses)` counts; anything else is dropped.
 *
 * **Use cases:** the leaf aggregate behind [ClassificationTree], and standalone
 * wherever a weighted class histogram, majority vote, or empirical class
 * distribution is wanted over a discrete stream.
 *
 * **Memory:** O([numClasses]); one adder cell per class.
 *
 * **Update:** O(1) per observation; one class-label resolution and a single
 * cell add.
 *
 * **Concurrency:** Independent striped cells with deterministic bucket
 * assignment. Exact under every level; increments commute and racing writers on
 * the same class share the cell.
 */
class ClassCountsStat(
    /** Number of classes; `value` must be exactly an integer in `[0, numClasses)`. */
    val numClasses: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<ClassCountsResult> {

    init {
        requireAtLeastTwoClasses(numClasses)
    }

    private val mode = concurrency.additiveMode()
    private val cells: StreamDoubleArray = mode.newDoubleArray(numClasses)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // isInertWeight, not `weight == 0.0`, which is false for NaN: a NaN weight would reach
        // `cells.add` and pin a class count to NaN permanently. A negative weight stays legal because
        // a count cell subtracts exactly, so retracting an earlier observation is a downdate.
        if (weight.isInertWeight()) return
        val c = value.asClassLabel(numClasses)
        if (c < 0) return
        cells.add(c, weight)
    }

    override fun read(timestampNanos: Long) = ClassCountsResult(
        numClasses,
        DoubleArray(numClasses) { cells.load(it) },
    )

    override fun merge(values: ClassCountsResult) {
        require(values.numClasses == numClasses) {
            "numClasses mismatch on merge: this=$numClasses, other=${values.numClasses}"
        }
        for (k in 0 until numClasses) cells.add(k, values.counts[k])
    }

    override fun reset() {
        for (k in 0 until numClasses) cells.store(k, 0.0)
    }

    override fun create(concurrency: Concurrency?) = ClassCountsStat(numClasses, concurrency ?: this.concurrency)
}

/** Chan-style parallel merge of two class-count aggregates: element-wise addition. */
internal fun mergeCC(a: ClassCountsResult, b: ClassCountsResult): ClassCountsResult {
    require(a.numClasses == b.numClasses) { "numClasses mismatch: ${a.numClasses} vs ${b.numClasses}" }
    val out = DoubleArray(a.numClasses) { a.counts[it] + b.counts[it] }
    return ClassCountsResult(a.numClasses, out)
}

/**
 * Inverse of [mergeCC]: element-wise subtraction.
 *
 * Deliberately unclamped. A count cell downdates, so a class can legitimately hold a negative total,
 * and flooring each class separately would invent mass in one class to cancel a downdate in another.
 * Callers drop a residual with no weight left rather than relying on a per-class floor.
 */
internal fun subtractCC(total: ClassCountsResult, part: ClassCountsResult): ClassCountsResult {
    require(total.numClasses == part.numClasses) {
        "numClasses mismatch: ${total.numClasses} vs ${part.numClasses}"
    }
    val out = DoubleArray(total.numClasses) { total.counts[it] - part.counts[it] }
    return ClassCountsResult(total.numClasses, out)
}

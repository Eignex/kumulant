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
 * Series stat over discrete class-index inputs. `update(value, weight)` interprets
 * `value.toInt()` as the class index; out-of-range values are dropped. Each class
 * cell is an independent striped atomic adder so updates commute under any
 * [Concurrency] level.
 */
class ClassCountsStat(
    /** Number of classes; `value.toInt()` must round into `[0, numClasses)`. */
    val numClasses: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<ClassCountsResult> {

    init {
        requireAtLeastTwoClasses(numClasses)
    }

    private val mode = concurrency.additiveMode()
    private val cells: StreamDoubleArray = mode.newDoubleArray(numClasses)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // The guard checked the NaN *value* but not the NaN *weight*, and `weight == 0.0` is false for
        // NaN, so a NaN weight reached `cells.add` and pinned a class count to NaN permanently. A
        // negative weight stays legal: a count cell subtracts exactly, so retracting an observation
        // that was folded in earlier is a downdate rather than corruption. The label now goes through
        // asClassLabel like every other classifier's, which is what rejects 1.5 as a class index.
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

/** Inverse of [mergeCC]: element-wise subtraction, floor at zero. */
internal fun subtractCC(total: ClassCountsResult, part: ClassCountsResult): ClassCountsResult {
    require(total.numClasses == part.numClasses) {
        "numClasses mismatch: ${total.numClasses} vs ${part.numClasses}"
    }
    val out = DoubleArray(total.numClasses) { (total.counts[it] - part.counts[it]).coerceAtLeast(0.0) }
    return ClassCountsResult(total.numClasses, out)
}

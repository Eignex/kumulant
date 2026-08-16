package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

/**
 * Snapshot of a weighted K-by-K confusion matrix indexed as `counts[predicted][truth]`.
 *
 * Exposes the family of confusion-matrix metrics as derived getters: per-class
 * precision/recall/F1, macro and micro averages, accuracy, and Matthews
 * correlation coefficient (defined for any K). Empty rows or columns yield 0
 * for the corresponding rate rather than NaN, which keeps macro averages
 * well-defined while a stream is warming up.
 */
@Serializable
@SerialName("ConfusionMatrixResult")
data class ConfusionMatrixResult(
    /** Number of classes. */
    val numClasses: Int,
    /** Row-major `numClasses * numClasses` weights: `counts[pred * numClasses + truth]`. */
    val counts: DoubleArray,
) : Result {

    init {
        require(numClasses > 0) { "numClasses must be > 0; got $numClasses" }
        require(counts.size == numClasses * numClasses) {
            "counts must be numClasses^2 = ${numClasses * numClasses}; got ${counts.size}"
        }
    }

    /** `counts[pred][truth]`. */
    fun count(predicted: Int, truth: Int): Double = counts[predicted * numClasses + truth]

    /** Total weight across all cells. */
    val totalWeights: Double get() {
        var s = 0.0
        for (c in counts) s += c
        return s
    }

    /** Sum of the diagonal (correctly classified weight). */
    val correct: Double get() {
        var s = 0.0
        for (i in 0 until numClasses) s += count(i, i)
        return s
    }

    /** Fraction of correctly classified weight; 0 on an empty stream. */
    val accuracy: Double get() = totalWeights.let { if (it > 0.0) correct / it else 0.0 }

    /** Predicted-class total: weight of all rows where prediction = [c]. */
    fun predictedTotal(c: Int): Double {
        var s = 0.0
        for (t in 0 until numClasses) s += count(c, t)
        return s
    }

    /** True-class total: weight of all columns where truth = [c]. */
    fun actualTotal(c: Int): Double {
        var s = 0.0
        for (p in 0 until numClasses) s += count(p, c)
        return s
    }

    /** Per-class precision: `TP / (TP + FP)`; 0 when no predictions for class [c]. */
    fun precision(c: Int): Double {
        val pt = predictedTotal(c)
        return if (pt > 0.0) count(c, c) / pt else 0.0
    }

    /** Per-class recall: `TP / (TP + FN)`; 0 when no truth observations for class [c]. */
    fun recall(c: Int): Double {
        val at = actualTotal(c)
        return if (at > 0.0) count(c, c) / at else 0.0
    }

    /** Per-class F1: harmonic mean of precision and recall; 0 when both are 0. */
    fun f1(c: Int): Double {
        val p = precision(c)
        val r = recall(c)
        val s = p + r
        return if (s > 0.0) 2.0 * p * r / s else 0.0
    }

    /** Macro precision: unweighted mean of per-class precision. */
    val macroPrecision: Double get() {
        var s = 0.0
        for (c in 0 until numClasses) s += precision(c)
        return s / numClasses
    }

    /** Macro recall: unweighted mean of per-class recall. */
    val macroRecall: Double get() {
        var s = 0.0
        for (c in 0 until numClasses) s += recall(c)
        return s / numClasses
    }

    /** Macro F1: unweighted mean of per-class F1. */
    val macroF1: Double get() {
        var s = 0.0
        for (c in 0 until numClasses) s += f1(c)
        return s / numClasses
    }

    /**
     * Matthews correlation coefficient generalised to K classes (Gorodkin 2004).
     * Returns 0 when either margin is degenerate.
     */
    val mcc: Double get() {
        val n = totalWeights
        if (n <= 0.0) return 0.0
        val tk = DoubleArray(numClasses) { actualTotal(it) }
        val pk = DoubleArray(numClasses) { predictedTotal(it) }
        var dot = 0.0
        for (c in 0 until numClasses) dot += count(c, c)
        val numerator = dot * n - run {
            var s = 0.0
            for (k in 0 until numClasses) s += tk[k] * pk[k]
            s
        }
        var sumPk2 = 0.0
        var sumTk2 = 0.0
        for (k in 0 until numClasses) {
            sumPk2 += pk[k] * pk[k]
            sumTk2 += tk[k] * tk[k]
        }
        val denom = sqrt((n * n - sumPk2) * (n * n - sumTk2))
        return if (denom > 0.0) numerator / denom else 0.0
    }

    override fun equals(other: Any?): Boolean = other is ConfusionMatrixResult &&
        numClasses == other.numClasses && counts.contentEquals(other.counts)

    override fun hashCode(): Int = 31 * numClasses + counts.contentHashCode()
}

/**
 * Streaming K-by-K confusion matrix over paired `(predictedClass, trueClass)`
 * observations. Inputs are class indices in `[0, numClasses)`; the doubles are
 * truncated to ints via `toInt()` and out-of-range pairs are ignored. Use for
 * online classifier evaluation; pair with the metric getters on
 * [ConfusionMatrixResult] for accuracy, per-class P/R/F1, macro F1, and MCC.
 *
 * **Memory:** O([numClasses]^2) striped atomic cells.
 *
 * **Update:** O(1) per pair (one cell increment).
 *
 * **Concurrency:** Independent striped atomic adds on a single cell per
 * update; lock-free and exact under every [Concurrency] level.
 */
class ConfusionMatrixStat(
    /** Number of classes; class indices are `[0, numClasses)`. */
    val numClasses: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<ConfusionMatrixResult> {

    init {
        require(numClasses > 0) { "numClasses must be > 0; got $numClasses" }
    }

    private val mode = concurrency.additiveMode()
    private val cells: Array<StreamDouble> = Array(numClasses * numClasses) { mode.newDouble(0.0) }

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        if (x.isNaN() || y.isNaN()) return
        val p = x.toInt()
        val t = y.toInt()
        // toInt() truncates toward zero, so anything in (-1, 0) became class 0 and then passed the
        // range check - scoring an out-of-range prediction as a correct class-0 one. The KDoc
        // promises out-of-range pairs are ignored, so require the label to be the integer it claims.
        if (p.toDouble() != x || t.toDouble() != y) return
        if (p !in 0 until numClasses || t !in 0 until numClasses) return
        cells[p * numClasses + t].add(weight)
    }

    override fun read(timestampNanos: Long) = ConfusionMatrixResult(
        numClasses,
        DoubleArray(cells.size) { cells[it].load() },
    )

    override fun merge(values: ConfusionMatrixResult) {
        require(values.numClasses == numClasses) {
            "numClasses mismatch on merge: this=$numClasses, other=${values.numClasses}"
        }
        for (i in cells.indices) cells[i].add(values.counts[i])
    }

    override fun reset() {
        for (c in cells) c.store(0.0)
    }

    override fun create(concurrency: Concurrency?) = ConfusionMatrixStat(numClasses, concurrency ?: this.concurrency)
}

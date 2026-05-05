package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Per-bin per-class weighted counts, row-major. `counts[binIndex * numClasses + classIndex]`
 * holds `Σ wᵢ` for `(binIndex, classIndex)` pairs. Powers Gini/entropy splits in
 * RF classification trees.
 */
@Serializable
@SerialName("ClassHistogram")
data class ClassHistogramResult(
    val numBins: Int,
    val numClasses: Int,
    val counts: DoubleArray,
) : Result {
    init {
        require(counts.size == numBins * numClasses) {
            "counts size ${counts.size} does not match numBins * numClasses = ${numBins * numClasses}"
        }
    }

    /** Read the weighted count at `(binIndex, classIndex)`. */
    fun count(binIndex: Int, classIndex: Int): Double {
        require(binIndex in 0 until numBins && classIndex in 0 until numClasses)
        return counts[binIndex * numClasses + classIndex]
    }

    override fun equals(other: Any?): Boolean = other is ClassHistogramResult &&
        numBins == other.numBins && numClasses == other.numClasses && counts.contentEquals(other.counts)

    override fun hashCode(): Int = 31 * (31 * numBins + numClasses) + counts.contentHashCode()
}

/**
 * Per-bin per-class count accumulator for RF classification splits.
 *
 * Pre-binned input: caller maps feature value → integer bin once, and class
 * label → integer index once. Single atomic add per update; lock-free.
 */
class ClassHistogram(
    val numBins: Int,
    val numClasses: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : Stat<ClassHistogramResult> {

    init {
        require(numBins > 0) { "numBins must be > 0; got $numBins" }
        require(numClasses > 0) { "numClasses must be > 0; got $numClasses" }
    }

    private val mode = concurrency.additiveMode()
    private val cells: Array<StreamDouble> =
        Array(numBins * numClasses) { mode.newDouble(0.0) }

    fun update(binIndex: Int, classIndex: Int, weight: Double = 1.0) {
        require(binIndex in 0 until numBins) {
            "binIndex $binIndex outside [0, $numBins)"
        }
        require(classIndex in 0 until numClasses) {
            "classIndex $classIndex outside [0, $numClasses)"
        }
        if (weight == 0.0) return
        cells[binIndex * numClasses + classIndex].add(weight)
    }

    override fun read(timestampNanos: Long) =
        ClassHistogramResult(numBins, numClasses, DoubleArray(cells.size) { cells[it].load() })

    override fun merge(values: ClassHistogramResult) {
        require(values.numBins == numBins && values.numClasses == numClasses) {
            "Shape mismatch on merge: this=($numBins,$numClasses), other=(${values.numBins},${values.numClasses})"
        }
        for (i in cells.indices) cells[i].add(values.counts[i])
    }

    override fun reset() {
        for (c in cells) c.store(0.0)
    }

    override fun create(concurrency: Concurrency?) =
        ClassHistogram(numBins, numClasses, concurrency ?: this.concurrency)
}

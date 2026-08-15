@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.anomaly

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stream.Mutex
import com.eignex.kumulant.stream.NoopMutex
import com.eignex.kumulant.stream.PlatformMutex
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.additiveMode
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.random.Random

/**
 * Per-feature `(low, high)` range used to seed random thresholds at tree construction.
 */
@Serializable
@SerialName("FeatureRange")
data class FeatureRange(
    /** Inclusive lower bound of the feature's expected value range. */
    val low: Double,
    /** Exclusive upper bound of the feature's expected value range. */
    val high: Double,
) {
    init {
        require(high > low) { "high must be > low; got low=$low high=$high" }
    }
}

/**
 * Snapshot of [HalfSpaceTreesStat]: the immutable tree structure plus the
 * reference-window per-leaf masses. Exposes [score] to evaluate a query
 * vector against the trees' frozen distribution.
 */
@Serializable
@SerialName("HalfSpaceTreesResult")
data class HalfSpaceTreesResult(
    /** Number of input features. */
    val featureSize: Int,
    /** Number of trees in the ensemble. */
    val numTrees: Int,
    /** Depth of each tree; each tree has `2^height` leaves. */
    val height: Int,
    /** Cumulative observation weight folded into the reference window. */
    override val totalWeights: Double,
    /** Flat `numTrees * (2^height - 1)` array of per-internal-node feature indices. */
    val featureIndices: IntArray,
    /** Flat `numTrees * (2^height - 1)` array of per-internal-node split thresholds. */
    val thresholds: DoubleArray,
    /** Flat `numTrees * 2^height` array of per-leaf masses summed over the reference window. */
    val referenceMass: DoubleArray,
) : HasObservationCount {

    private val numInternal: Int get() = (1 shl height) - 1
    private val numLeaves: Int get() = 1 shl height

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(numTrees > 0) { "numTrees must be positive" }
        require(height > 0) { "height must be positive" }
        require(featureIndices.size == numTrees * numInternal) {
            "featureIndices must have length ${numTrees * numInternal}; got ${featureIndices.size}"
        }
        require(thresholds.size == numTrees * numInternal) {
            "thresholds must have length ${numTrees * numInternal}; got ${thresholds.size}"
        }
        require(referenceMass.size == numTrees * numLeaves) {
            "referenceMass must have length ${numTrees * numLeaves}; got ${referenceMass.size}"
        }
    }

    /**
     * Half-Space-Trees anomaly score for [x]. Routes [x] through every tree to a
     * leaf, sums `referenceMass[leaf] * 2^depth` across trees. Higher score
     * means [x] falls into densely populated regions of the reference window;
     * i.e. it looks normal. Lower score flags an anomaly.
     */
    fun score(x: VectorView): Double {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        var total = 0.0
        val depthFactor = 1 shl height
        for (t in 0 until numTrees) {
            val leafIdx = routeToLeaf(t, x)
            total += referenceMass[t * numLeaves + leafIdx] * depthFactor
        }
        return total
    }

    private fun routeToLeaf(treeIdx: Int, x: VectorView): Int {
        val treeOffset = treeIdx * numInternal
        var node = 0
        repeat(height) {
            val featureIdx = featureIndices[treeOffset + node]
            val threshold = thresholds[treeOffset + node]
            node = if (x[featureIdx] < threshold) 2 * node + 1 else 2 * node + 2
        }
        return node - numInternal
    }

    override fun equals(other: Any?): Boolean = other is HalfSpaceTreesResult &&
        featureSize == other.featureSize && numTrees == other.numTrees && height == other.height &&
        totalWeights == other.totalWeights &&
        featureIndices.contentEquals(other.featureIndices) &&
        thresholds.contentEquals(other.thresholds) &&
        referenceMass.contentEquals(other.referenceMass)

    override fun hashCode(): Int {
        var h = featureSize * 31 + numTrees
        h = 31 * h + height
        h = 31 * h + totalWeights.hashCode()
        h = 31 * h + featureIndices.contentHashCode()
        h = 31 * h + thresholds.contentHashCode()
        h = 31 * h + referenceMass.contentHashCode()
        return h
    }
}

/**
 * Online Half-Space-Trees anomaly detector (Tan, Ting & Liu 2011). An ensemble
 * of pre-built random half-space trees of fixed depth [height]; each internal
 * node picks a random feature and a random threshold from
 * [featureRanges][HalfSpaceTreesStat.featureRanges] at construction. Trees do
 * not grow; the algorithm tracks two mass profiles per leaf; a *reference*
 * window and the *latest* window; and swaps them every [windowSize]
 * observations. The anomaly score is computed from the reference profile.
 *
 * **Use cases:** non-parametric multivariate anomaly detection on streams
 * where the data distribution may shift slowly (reference window keeps the
 * recent profile fresh). Cheap, fully parallel across trees.
 *
 * **Memory:** O([numTrees] * 2^[height]); two per-leaf mass arrays plus the
 * immutable tree structure.
 *
 * **Update:** O([numTrees] * [height]) per observation (one tree-walk per
 * tree, no growth).
 *
 * **Concurrency:** Per-leaf mass updates are striped atomic adds and commute.
 * The periodic window swap (reference <- latest; latest <- 0) takes a single
 * lock fired once every [windowSize] observations.
 */
class HalfSpaceTreesStat(
    /** Number of input features. */
    val featureSize: Int,
    /** Per-feature value ranges used to draw random split thresholds at tree build time. */
    val featureRanges: List<FeatureRange>,
    /** Number of trees in the ensemble. */
    val numTrees: Int = 25,
    /** Depth of each tree; each tree has `2^height` leaves. */
    val height: Int = 8,
    /** Observations per window before the reference profile rotates. */
    val windowSize: Int = 250,
    /** PRNG seed for reproducible tree construction. */
    val randomSeed: Int = 0,
    override val concurrency: Concurrency = Concurrency.None,
) : VectorStat<HalfSpaceTreesResult> {

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(featureRanges.size == featureSize) {
            "featureRanges must have length $featureSize; got ${featureRanges.size}"
        }
        require(numTrees > 0) { "numTrees must be positive" }
        // Bound the height, not just its sign: the node counts below are `1 shl height` in Int, so
        // 31 wrapped negative (OutOfMemoryError in the constructor) and 32 wrapped to 1, leaving
        // numInternal == 0 and an ArrayIndexOutOfBounds on the first update.
        require(height in 1..MAX_HEIGHT) { "height must be in 1..$MAX_HEIGHT, got $height" }
        require(windowSize > 0) { "windowSize must be positive" }
        require(numTrees.toLong() * (1L shl height) <= Int.MAX_VALUE) {
            "numTrees * 2^height must fit in an Int; $numTrees * 2^$height overflows"
        }
    }

    private val numInternal: Int = (1 shl height) - 1
    private val numLeaves: Int = 1 shl height

    // Immutable tree structure: drawn once at construction.
    private val featureIndices: IntArray
    private val thresholds: DoubleArray

    init {
        val rng = Random(randomSeed)
        val total = numTrees * numInternal
        featureIndices = IntArray(total)
        thresholds = DoubleArray(total)
        for (i in 0 until total) {
            val f = rng.nextInt(featureSize)
            featureIndices[i] = f
            val r = featureRanges[f]
            thresholds[i] = r.low + rng.nextDouble() * (r.high - r.low)
        }
    }

    // The mass arrays and the weight cell are pure accumulations, so they take
    // additiveMode; the window counter needs CAS for addAndGet, so it takes
    // monotonicMode. Previously all four used welfordMode(), which returns AtomicMode
    // only for Concurrency.Relaxed and SerialMode for None, Strict and HighWrite. Since
    // this stat has no lock over the update path, that left Strict and HighWrite doing
    // plain non-volatile reads and writes: increments were lost outright and the counter
    // update was not guaranteed visible to other cores, so rotateWindow fired at the
    // wrong cadence and scores were computed against a half-built reference profile.
    private val massMode = concurrency.additiveMode()
    private val counterMode = concurrency.monotonicMode()
    private val swapLock: Mutex = if (concurrency == Concurrency.None) NoopMutex else PlatformMutex()

    private val referenceMass: StreamDoubleArray = massMode.newDoubleArray(numTrees * numLeaves)
    private val latestMass: StreamDoubleArray = massMode.newDoubleArray(numTrees * numLeaves)
    private val windowCounter: StreamLong = counterMode.newLong(0L)
    private val totalWeightsCell = massMode.newDouble(0.0)

    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        require(vector.size == featureSize) { "vector.size=${vector.size}, expected $featureSize" }
        if (weight <= 0.0) return
        for (t in 0 until numTrees) {
            val leafIdx = routeToLeaf(t, vector)
            latestMass.add(t * numLeaves + leafIdx, weight)
        }
        totalWeightsCell.add(weight)
        val n = windowCounter.addAndGet(1L)
        if (n >= windowSize) rotateWindow()
    }

    private fun rotateWindow() = swapLock.guarded {
        if (windowCounter.load() < windowSize) return@guarded
        windowCounter.store(0L)
        for (i in 0 until numTrees * numLeaves) {
            val latest = latestMass.load(i)
            referenceMass.store(i, latest)
            latestMass.store(i, 0.0)
        }
    }

    private fun routeToLeaf(treeIdx: Int, x: VectorView): Int {
        val treeOffset = treeIdx * numInternal
        var node = 0
        repeat(height) {
            val featureIdx = featureIndices[treeOffset + node]
            val threshold = thresholds[treeOffset + node]
            node = if (x[featureIdx] < threshold) 2 * node + 1 else 2 * node + 2
        }
        return node - numInternal
    }

    override fun read(timestampNanos: Long): HalfSpaceTreesResult = HalfSpaceTreesResult(
        featureSize = featureSize,
        numTrees = numTrees,
        height = height,
        totalWeights = totalWeightsCell.load(),
        featureIndices = featureIndices.copyOf(),
        thresholds = thresholds.copyOf(),
        referenceMass = DoubleArray(numTrees * numLeaves) { referenceMass.load(it) },
    )

    /**
     * Merge folds another snapshot's reference-window masses into both this stat's reference and
     * latest windows. Tree structures must match exactly; if they don't (e.g. different seeds), the
     * merge throws.
     *
     * The reference window is what [score] reads, so folding into the latest window alone left a
     * merged model scoring every input maximally anomalous until `windowSize` fresh observations
     * rotated the window - while reporting the source's `totalWeights`, so it looked trained. The
     * incoming masses *are* a reference profile, so they belong in the reference window too.
     */
    override fun merge(values: HalfSpaceTreesResult) {
        require(values.numTrees == numTrees && values.height == height && values.featureSize == featureSize) {
            "merge: shape mismatch"
        }
        require(values.featureIndices.contentEquals(featureIndices) && values.thresholds.contentEquals(thresholds)) {
            "merge: tree structure must match (different seeds will produce different trees)"
        }
        require(values.referenceMass.size == numTrees * numLeaves) {
            "merge: expected ${numTrees * numLeaves} masses, got ${values.referenceMass.size}"
        }
        for (i in 0 until numTrees * numLeaves) {
            referenceMass.add(i, values.referenceMass[i])
            latestMass.add(i, values.referenceMass[i])
        }
        totalWeightsCell.add(values.totalWeights)
    }

    override fun reset() = swapLock.guarded {
        for (i in 0 until numTrees * numLeaves) {
            referenceMass.store(i, 0.0)
            latestMass.store(i, 0.0)
        }
        windowCounter.store(0L)
        totalWeightsCell.store(0.0)
    }

    override fun create(concurrency: Concurrency?): HalfSpaceTreesStat = HalfSpaceTreesStat(
        featureSize = featureSize,
        featureRanges = featureRanges,
        numTrees = numTrees,
        height = height,
        windowSize = windowSize,
        randomSeed = randomSeed,
        concurrency = concurrency ?: this.concurrency,
    )

    private companion object {
        /** 2^30 leaves is already far past anything useful and keeps `1 shl height` positive. */
        const val MAX_HEIGHT = 30
    }
}

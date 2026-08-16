package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.math.nextPoissonOne
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ceil
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Online random-forest classifier; the classification counterpart of
 * [RandomForestRegressionStat]. Same diversity tricks (Oza & Russell bagging,
 * per-leaf mtry), but per-tree leaves are [ClassCountsResult] and ensemble
 * predictions average per-class probabilities across trees.
 */
class RandomForestClassifierStat(
    override val featureSize: Int,
    /** Number of classes; `y` must round to `[0, numClasses)`. */
    val numClasses: Int,
    /** Candidate split pool. Used by every tree; the per-leaf mtry filter draws from here. */
    val splitCandidates: List<SerializableSplit>,
    /** Trees in the forest. */
    val nbrTrees: Int = 10,
    config: ClassificationTreeConfig = ClassificationTreeConfig(),
    /** Oza & Russell online bagging: per-tree Poisson(1) reweighting at update time. */
    val bagging: Boolean = true,
    override val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: () -> SeriesStat<ClassCountsResult> = { ClassCountsStat(numClasses, concurrency) },
    randomSeed: Int = 0,
) : RegressionStat<ForestClassificationResult> {

    init {
        require(featureSize > 0) { "featureSize must be positive, got $featureSize" }
        require(numClasses >= 2) { "numClasses must be >= 2; got $numClasses" }
        require(nbrTrees > 0) { "nbrTrees must be positive, got $nbrTrees" }
    }

    /** [ClassificationTreeConfig] with [ClassificationTreeConfig.mtry] defaulted to `ceil(sqrt(p))` when null. */
    val config: ClassificationTreeConfig = config.copy(mtry = config.mtry ?: defaultMtry(splitCandidates.size))

    private val seedRng = Random(randomSeed)
    private val baggingRng = Random(seedRng.nextInt())
    private var trees: Array<ClassificationTree> = Array(nbrTrees) { newTree() }

    private fun newTree(): ClassificationTree = ClassificationTree(
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        config = this.config,
        concurrency = concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = seedRng.nextInt(),
    )

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        if (weight <= 0.0 || y.isNaN()) return
        val c = y.toInt()
        if (c !in 0 until numClasses) return
        if (!bagging) {
            for (t in trees) t.update(x, c, weight)
            return
        }
        for (t in trees) {
            val k = baggingRng.nextPoissonOne()
            if (k > 0) t.update(x, c, weight * k)
        }
    }

    override fun read(timestampNanos: Long): ForestClassificationResult =
        ForestClassificationResult(numClasses, trees.map { TreeClassificationResult(it.rootNode().snapshot()) })

    override fun merge(values: ForestClassificationResult) {
        require(values.trees.size == trees.size) {
            "merge: forest size mismatch (${values.trees.size} vs ${trees.size})"
        }
        require(values.numClasses == numClasses) {
            "merge: numClasses mismatch (${values.numClasses} vs $numClasses)"
        }
        for (i in trees.indices) trees[i].mergeSnapshot(values.trees[i].root)
    }

    override fun reset() {
        trees = Array(nbrTrees) { newTree() }
    }

    override fun create(concurrency: Concurrency?): RandomForestClassifierStat = RandomForestClassifierStat(
        featureSize = featureSize,
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        nbrTrees = nbrTrees,
        config = config,
        bagging = bagging,
        concurrency = concurrency ?: this.concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = seedRng.nextInt(),
    )

    /** Live underlying classification trees. Use for inspection. */
    fun trees(): List<ClassificationTree> = trees.toList()

    private fun defaultMtry(p: Int): Int = if (p <= 0) 0 else ceil(sqrt(p.toDouble())).toInt().coerceAtLeast(1)
}

/** Snapshot of a [RandomForestClassifierStat]: per-tree immutable snapshots plus
 *  ensemble-aware predict helpers. */
@Serializable
@SerialName("ForestClassificationResult")
data class ForestClassificationResult(
    /** Number of classes shared by every tree. */
    val numClasses: Int,
    /** Per-tree immutable snapshots; non-empty. */
    val trees: List<TreeClassificationResult>,
) : Result {
    init {
        require(trees.isNotEmpty()) { "ForestClassificationResult requires at least one tree" }
        require(trees.all { it.numClasses == numClasses }) { "all trees must share numClasses=$numClasses" }
    }

    /** Average per-class probability across trees for the leaf each tree routes [x] to. */
    fun probabilities(x: VectorView): DoubleArray {
        val acc = DoubleArray(numClasses)
        for (t in trees) {
            val p = t.probabilities(x)
            for (k in 0 until numClasses) acc[k] += p[k]
        }
        val inv = 1.0 / trees.size
        for (k in 0 until numClasses) acc[k] *= inv
        return acc
    }

    /** Argmax over [probabilities]. */
    fun predict(x: VectorView): Int {
        val p = probabilities(x)
        var best = 0
        for (k in 1 until numClasses) if (p[k] > p[best]) best = k
        return best
    }

    /** Sum of per-tree root totalWeights. */
    val totalWeights: Double get() = trees.sumOf { it.totalWeights }
}

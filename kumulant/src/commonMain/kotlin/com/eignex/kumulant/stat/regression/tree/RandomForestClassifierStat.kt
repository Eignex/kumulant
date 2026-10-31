package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.core.requireAtLeastTwoClasses
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.math.argMaxOf
import com.eignex.kumulant.stat.StatDefaults
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Online random-forest classifier; the classification counterpart of
 * [RandomForestRegressionStat]. Same diversity tricks (Oza & Russell bagging,
 * per-leaf mtry), but per-tree leaves are [ClassCountsResult] and ensemble
 * predictions average per-class probabilities across trees.
 *
 * **Use cases:** online classification wanting ensembled diversity and a
 * per-class probability estimate averaged across trees. Reach for
 * [DecisionTreeClassifierStat] alone when a single tree suffices.
 *
 * **Memory:** O([nbrTrees] · single-tree memory); see
 * [DecisionTreeClassifierStat]. Heavier but parallelisable.
 *
 * **Update:** O([nbrTrees] · depth) per observation; each tree's update is
 * independent. Under [bagging] = true, each tree applies a fresh
 * Poisson(1)-reweighted version of the update.
 *
 * **Concurrency:** Inherits [DecisionTreeClassifierStat]'s per-tree
 * concurrency model. Trees are updated sequentially within a single
 * `update()` call (no inner parallelism); concurrent callers each contend for
 * each tree's split lock independently.
 */
class RandomForestClassifierStat(
    override val featureSize: Int,
    /** Number of classes; `y` must round to `[0, numClasses)`. */
    val numClasses: Int,
    /** Candidate split pool. Used by every tree; the per-leaf mtry filter draws from here. */
    val splitCandidates: List<SerializableSplit>,
    /** Trees in the forest. */
    val nbrTrees: Int = StatDefaults.FOREST_SIZE,
    config: ClassificationTreeConfig = ClassificationTreeConfig(),
    /** Oza & Russell online bagging: per-tree Poisson(1) reweighting at update time. */
    val bagging: Boolean = StatDefaults.FOREST_BAGGING,
    override val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: ((Concurrency) -> SeriesStat<ClassCountsResult>)? = null,
    randomSeed: Int = StatDefaults.TREE_RANDOM_SEED,
) : RegressionStat<ForestClassificationResult> {

    init {
        requirePositiveFeatureSize(featureSize)
        requireAtLeastTwoClasses(numClasses)
    }

    /** [ClassificationTreeConfig] with [ClassificationTreeConfig.mtry] defaulted to `ceil(sqrt(p))` when null. */
    val config: ClassificationTreeConfig = config.copy(mtry = config.mtry ?: defaultMtry(splitCandidates.size))

    // Per-instance resolution binds leaf arms to each replica concurrency level.
    private val armFactory: () -> SeriesStat<ClassCountsResult> = {
        leafArmFactory?.invoke(concurrency) ?: ClassCountsStat(numClasses, concurrency)
    }

    private val forest = BaggedTrees(nbrTrees, bagging, concurrency, randomSeed) { seed ->
        ClassificationTree(
            numClasses = numClasses,
            splitCandidates = splitCandidates,
            config = this.config,
            concurrency = concurrency,
            leafArmFactory = armFactory,
            randomSeed = seed,
        )
    }
    private val trees: List<ClassificationTree> get() = forest.trees

    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        if (!forest.accepts(weight)) return
        val c = y.asClassLabel(numClasses)
        if (c < 0) return
        for (t in trees) {
            val k = forest.multiplicity()
            if (k > 0) t.update(x, c, weight * k)
        }
    }

    override fun read(timestampNanos: Long): ForestClassificationResult =
        ForestClassificationResult(numClasses, trees.map { TreeClassificationResult(it.rootNode().snapshot()) })

    override fun validateMerge(values: ForestClassificationResult) {
        require(values.trees.size == trees.size) {
            "merge: forest size mismatch (${values.trees.size} vs ${trees.size})"
        }
        require(values.numClasses == numClasses) {
            "merge: numClasses mismatch (${values.numClasses} vs $numClasses)"
        }
    }

    override fun merge(values: ForestClassificationResult, timestampNanos: Long) {
        validateMerge(values)
        for (i in trees.indices) trees[i].mergeSnapshot(values.trees[i].root)
    }

    /** Restores empty state and restarts the seed streams for reproducible growth. */
    override fun reset() = forest.reset()

    override fun create(concurrency: Concurrency?): RandomForestClassifierStat = RandomForestClassifierStat(
        featureSize = featureSize,
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        nbrTrees = nbrTrees,
        config = config,
        bagging = bagging,
        concurrency = concurrency ?: this.concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = forest.childSeed(),
    )

    /** Live underlying classification trees. Use for inspection. */
    fun trees(): List<ClassificationTree> = trees
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
    fun probabilities(x: Vector): DoubleArray {
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
    fun predict(x: Vector): Int {
        val p = probabilities(x)
        return argMaxOf(numClasses) { k -> p[k] }
    }

    /** Sum of per-tree root totalWeights. */
    val totalWeights: Double get() = trees.sumOf { it.totalWeights }
}

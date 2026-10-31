package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Online random-forest regressor; a population of [RegressionTree]s sharing the candidate-split
 * pool. Diversity comes from:
 *
 *  - **Oza & Russell online bagging**: per-tree Poisson(1) reweighting at every update.
 *  - **Per-leaf mtry**: each tree's audit leaves consider a random subset of the
 *    candidate splits, drawn at leaf birth from the tree's own RNG.
 *
 * Snapshot is a [ForestRegressionResult] carrying every per-tree [TreeRegressionResult];
 * tree-aware posteriors merge per-tree leaf aggregates at score time.
 *
 * **Use cases:** non-linear contextual regression with built-in variance
 * estimation across trees; the natural backbone for Thompson-sampling
 * contextual bandits. Reach for [DecisionTreeRegressionStat] alone when a
 * single tree's predictions suffice and ensembled diversity isn't needed.
 *
 * **Memory:** O([nbrTrees] · single-tree memory); see
 * [DecisionTreeRegressionStat]. Heavier but parallelisable.
 *
 * **Update:** O([nbrTrees] · depth) per observation; each tree's update is
 * independent. Under [bagging] = true, each tree applies a fresh
 * Poisson(1)-reweighted version of the update.
 *
 * **Concurrency:** Inherits [DecisionTreeRegressionStat]'s per-tree
 * concurrency model. Trees are updated sequentially within a single
 * `update()` call (no inner parallelism); concurrent callers each contend
 * for each tree's split lock independently.
 */
class RandomForestRegressionStat(
    override val featureSize: Int,
    /** Candidate split pool. Used by every tree; the per-leaf mtry filter draws from here. */
    val splitCandidates: List<SerializableSplit>,
    /** Trees in the forest. */
    val nbrTrees: Int = StatDefaults.FOREST_SIZE,
    config: RegressionTreeConfig = RegressionTreeConfig(),
    /** Oza & Russell online bagging: per-tree Poisson(1) reweighting at update time. */
    val bagging: Boolean = StatDefaults.FOREST_BAGGING,
    override val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: ((Concurrency) -> SeriesStat<WeightedVarianceResult>)? = null,
    randomSeed: Int = StatDefaults.TREE_RANDOM_SEED,
) : RegressionStat<ForestRegressionResult> {

    init {
        requirePositiveFeatureSize(featureSize)
    }

    /** [RegressionTreeConfig] with [RegressionTreeConfig.mtry] defaulted to `ceil(sqrt(p))` when null. */
    val config: RegressionTreeConfig = config.copy(mtry = config.mtry ?: defaultMtry(splitCandidates.size))

    // Per-instance resolution binds leaf arms to each replica concurrency level.
    private val armFactory: () -> SeriesStat<WeightedVarianceResult> = {
        leafArmFactory?.invoke(concurrency) ?: VarianceStat(concurrency)
    }

    private val forest = BaggedTrees(nbrTrees, bagging, concurrency, randomSeed) { seed ->
        RegressionTree<Vector>(splitCandidates, this.config, concurrency, armFactory, seed)
    }
    private val trees: List<RegressionTree<Vector>> get() = forest.trees

    override fun update(x: Vector, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        if (!forest.accepts(weight)) return
        for (t in trees) {
            val k = forest.multiplicity()
            if (k > 0) t.update(x, y, weight * k)
        }
    }

    override fun read(timestampNanos: Long): ForestRegressionResult =
        ForestRegressionResult(trees.map { TreeRegressionResult(it.rootNode().snapshot()) })

    override fun validateMerge(values: ForestRegressionResult) {
        require(values.trees.size == trees.size) {
            "merge: forest size mismatch (${values.trees.size} vs ${trees.size})"
        }
    }

    override fun merge(values: ForestRegressionResult, timestampNanos: Long) {
        validateMerge(values)
        for (i in trees.indices) trees[i].mergeSnapshot(values.trees[i].root)
    }

    /** Restores empty state and restarts the seed streams for reproducible growth. */
    override fun reset() = forest.reset()

    override fun create(concurrency: Concurrency?): RandomForestRegressionStat = RandomForestRegressionStat(
        featureSize = featureSize,
        splitCandidates = splitCandidates,
        nbrTrees = nbrTrees,
        config = config,
        bagging = bagging,
        concurrency = concurrency ?: this.concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = forest.childSeed(),
    )

    /** Live leaves reached by [x], in tree order; validates the feature dimension. */
    fun findLeaves(x: Vector): List<RegressionLeafNode<Vector>> {
        x.requireFeatureSize(featureSize)
        return trees.map { it.findLeaf(x) }
    }

    /** Predict from live leaf statistics without snapshotting the forest's structure. */
    fun predict(x: Vector): Double = mergeForestEvidence(findLeaves(x).map { it.arm.read(0L) }).mean

    /** Live underlying trees. Use for inspection. */
    fun trees(): List<RegressionTree<Vector>> = trees

    /** Live tree at [index], including any replacement installed by [reset]. */
    fun tree(index: Int): RegressionTree<Vector> = trees[index]
}

/** Snapshot of a [RandomForestRegressionStat]: per-tree immutable snapshots. */
@Serializable
@SerialName("ForestRegressionResult")
data class ForestRegressionResult(
    /** Per-tree immutable snapshots; non-empty. */
    val trees: List<TreeRegressionResult>,
) : Result {
    init {
        require(trees.isNotEmpty()) { "ForestRegressionResult requires at least one tree" }
    }

    /** Merge the leaves that [x] routes to across every tree into a single weighted-
     *  variance aggregate. Useful for ensembled scoring. */
    fun findLeafMerged(x: Vector): WeightedVarianceResult =
        mergeForestEvidence(trees.map { it.findLeaf(x) }, independent = true)

    /** Correlation-adjusted leaf evidence for posterior scoring. */
    fun findLeafEvidence(x: Vector): WeightedVarianceResult = mergeForestEvidence(trees.map { it.findLeaf(x) })

    /** Mean of [findLeafMerged]. */
    fun predict(x: Vector): Double = findLeafMerged(x).mean

    /** Sum of per-tree root totalWeights; `nbrTrees * underlyingWeight` under bagging. */
    val totalWeights: Double get() = trees.sumOf { it.totalWeights }
}

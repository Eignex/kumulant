package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.nextPoissonOne
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ceil
import kotlin.math.sqrt
import kotlin.random.Random

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
    val nbrTrees: Int = 10,
    config: RegressionTreeConfig = RegressionTreeConfig(),
    /** Oza & Russell online bagging: per-tree Poisson(1) reweighting at update time. */
    val bagging: Boolean = true,
    override val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: () -> SeriesStat<WeightedVarianceResult> = { VarianceStat(concurrency) },
    randomSeed: Int = 0,
) : RegressionStat<ForestRegressionResult> {

    init {
        require(featureSize > 0) { "featureSize must be positive, got $featureSize" }
        require(nbrTrees > 0) { "nbrTrees must be positive, got $nbrTrees" }
    }

    /** [RegressionTreeConfig] with [RegressionTreeConfig.mtry] defaulted to `ceil(sqrt(p))` when null. */
    val config: RegressionTreeConfig = config.copy(mtry = config.mtry ?: defaultMtry(splitCandidates.size))

    private val seedRng = Random(randomSeed)
    private val baggingRng = Random(seedRng.nextInt())
    private var trees: Array<RegressionTree<VectorView>> = Array(nbrTrees) { newTree() }

    private fun newTree(): RegressionTree<VectorView> = RegressionTree(
        splitCandidates,
        this.config,
        concurrency,
        leafArmFactory,
        seedRng.nextInt(),
    )

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        if (!bagging) {
            for (t in trees) t.update(x, y, weight)
            return
        }
        for (t in trees) {
            val k = baggingRng.nextPoissonOne()
            if (k > 0) t.update(x, y, weight * k)
        }
    }

    override fun read(timestampNanos: Long): ForestRegressionResult =
        ForestRegressionResult(trees.map { TreeRegressionResult(it.rootNode().snapshot()) })

    override fun merge(values: ForestRegressionResult) {
        require(values.trees.size == trees.size) {
            "merge: forest size mismatch (${values.trees.size} vs ${trees.size})"
        }
        for (i in trees.indices) trees[i].mergeSnapshot(values.trees[i].root)
    }

    override fun reset() {
        trees = Array(nbrTrees) { newTree() }
    }

    override fun create(concurrency: Concurrency?): RandomForestRegressionStat = RandomForestRegressionStat(
        featureSize = featureSize,
        splitCandidates = splitCandidates,
        nbrTrees = nbrTrees,
        config = config,
        bagging = bagging,
        concurrency = concurrency ?: this.concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = seedRng.nextInt(),
    )

    /** Live underlying trees. Use for inspection. */
    fun trees(): List<RegressionTree<VectorView>> = trees.toList()

    private fun defaultMtry(p: Int): Int = if (p <= 0) 0 else ceil(sqrt(p.toDouble())).toInt().coerceAtLeast(1)
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
    fun findLeafMerged(x: VectorView): WeightedVarianceResult {
        var totalW = 0.0
        var mean = 0.0
        var sst = 0.0
        for (t in trees) {
            val leaf = t.findLeaf(x)
            val w2 = leaf.totalWeights
            if (w2 <= 0.0) continue
            val w1 = totalW
            val nextW = w1 + w2
            val delta = leaf.mean - mean
            mean += delta * (w2 / nextW)
            sst += leaf.variance * w2 + (delta * delta) * (w1 * w2 / nextW)
            totalW = nextW
        }
        val variance = if (totalW > 0.0) sst / totalW else 0.0
        return WeightedVarianceResult(totalW, mean, variance)
    }

    /** Mean of [findLeafMerged]. */
    fun predict(x: VectorView): Double = findLeafMerged(x).mean

    /** Sum of per-tree root totalWeights; `nbrTrees * underlyingWeight` under bagging. */
    val totalWeights: Double get() = trees.sumOf { it.totalWeights }
}

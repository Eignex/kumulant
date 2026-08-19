package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.F64VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.math.CounterRandom
import com.eignex.kumulant.math.nextPoissonOne
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
    val nbrTrees: Int = 10,
    config: RegressionTreeConfig = RegressionTreeConfig(),
    /** Oza & Russell online bagging: per-tree Poisson(1) reweighting at update time. */
    val bagging: Boolean = true,
    override val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: (() -> SeriesStat<WeightedVarianceResult>)? = null,
    randomSeed: Int = 0,
) : RegressionStat<ForestRegressionResult> {

    init {
        requirePositiveFeatureSize(featureSize)
        require(nbrTrees > 0) { "nbrTrees must be positive, got $nbrTrees" }
    }

    /** [RegressionTreeConfig] with [RegressionTreeConfig.mtry] defaulted to `ceil(sqrt(p))` when null. */
    val config: RegressionTreeConfig = config.copy(mtry = config.mtry ?: defaultMtry(splitCandidates.size))

    private val seedRng = CounterRandom(randomSeed.toLong(), concurrency)

    // Resolved per instance, not captured in the constructor default: create() has to be able
    // to rebind the default arm to the replica's concurrency.
    private val armFactory: () -> SeriesStat<WeightedVarianceResult> = leafArmFactory ?: { VarianceStat(concurrency) }

    // Drawn from on the update path, once per tree, with no lock over it - so it has to be a generator
    // that concurrent updates can share. See CounterRandom.
    private val baggingRng = CounterRandom(seedRng.childSeed(), concurrency)
    private var trees: Array<RegressionTree<F64VectorView>> = Array(nbrTrees) { newTree() }

    private fun newTree(): RegressionTree<F64VectorView> = RegressionTree(
        splitCandidates,
        this.config,
        concurrency,
        armFactory,
        seedRng.nextInt(),
    )

    override fun update(x: F64VectorView, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        // Return before drawing from baggingRng: an inert call that consumed one draw per tree would
        // desynchronise every later bagging draw and change the forest's predictions.
        if (weight.isInertWeight()) return
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

    /**
     * Restarts both seed streams, so the rebuilt forest is the one a fresh stat would have grown and
     * the bagging draws resume from the start rather than from wherever the old forest left off.
     * `reset` promises the equivalent of a fresh stat, not merely an emptied one.
     */
    override fun reset() {
        seedRng.reset()
        baggingRng.reset()
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
        randomSeed = seedRng.childSeed().toInt(),
    )

    /** Live underlying trees. Use for inspection. */
    fun trees(): List<RegressionTree<F64VectorView>> = trees.toList()
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
    fun findLeafMerged(x: F64VectorView): WeightedVarianceResult {
        // The `w2 <= 0.0` skip stays here rather than moving into mergeWVR, which short-circuits only
        // on an exactly-zero weight: a leaf left with negative total weight by an over-reaching
        // downdate would otherwise be folded in, and against a positive sibling of equal magnitude the
        // combined weight is zero, producing NaN for the whole forest.
        var acc = WeightedVarianceResult(0.0, 0.0, 0.0)
        for (t in trees) {
            val leaf = t.findLeaf(x)
            if (leaf.totalWeights <= 0.0) continue
            acc = mergeWVR(acc, leaf)
        }
        return acc
    }

    /** Mean of [findLeafMerged]. */
    fun predict(x: F64VectorView): Double = findLeafMerged(x).mean

    /** Sum of per-tree root totalWeights; `nbrTrees * underlyingWeight` under bagging. */
    val totalWeights: Double get() = trees.sumOf { it.totalWeights }
}

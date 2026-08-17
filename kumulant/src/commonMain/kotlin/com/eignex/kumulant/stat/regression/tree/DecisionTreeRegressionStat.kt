package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.random.Random

/**
 * Online VFDT decision-tree regressor; a piecewise-constant predictor over the feature
 * space, growing on the fly via the Hoeffding bound. Wraps a [RegressionTree] in the kumulant
 * [RegressionStat] contract so it composes with everything that consumes regressors
 * (the bandit family, schemas, op pipelines).
 *
 * Snapshots are immutable [TreeRegressionResult]s carrying the frozen split structure
 * and per-node weighted-variance aggregates; bandits pair this with a tree-aware
 * [com.eignex.kumulant.stat.regression.RegressionPosterior] (e.g. `MeanTreePosterior`
 * or `ThompsonTreePosterior`) to score arms at choose time.
 *
 * Reward encoding lives at the call site; pre-transform `y` (e.g. `ln(y)`) before
 * [update]. The internal leaf accumulator is fixed to [VarianceStat]'s
 * [WeightedVarianceResult] so the [VarianceReduction] split metric applies.
 *
 * **Use cases:** non-linear regression where the relationship between context
 * and target is piecewise constant or step-like; bandit reward modelling,
 * contextual stratification, anything where linear regression would miss the
 * structure. Reach for [RandomForestRegressionStat] for ensembled diversity.
 *
 * **Memory:** O(nodes · splitCandidates); a [VarianceStat] per node plus
 * per-audit-leaf candidate accumulators. Bounded by [RegressionTreeConfig.maxNodes].
 *
 * **Update:** O(depth) per observation; a tree walk to the destination leaf,
 * then an arm update at that leaf. Splits fire at most once every
 * [RegressionTreeConfig.splitPeriod] observations per audit leaf.
 *
 * **Concurrency:** The hot update path touches exactly one accumulator; the
 * leaf the observation routes to. Internal split nodes carry no live arm; subtree
 * aggregates (`rootSnapshot`, `TreeSplitResult.value`) are derived by combining
 * descendants at snapshot time. Each leaf arm is a [VarianceStat] honouring
 * [Concurrency], so multiple threads landing in different leaves never contend.
 * Split conversion takes a per-tree lock fired only at split decisions. Predictions
 * (the load-bearing consumer for bandits) are race-free; the root-level aggregate
 * `TreeRegressionResult.totalWeights` / `rootMean` is best-effort under concurrent
 * growth and may drift by a few ULPs of the workload; single-threaded runs are
 * exact. See [RegressionTree] for the full concurrency design.
 */
class DecisionTreeRegressionStat(
    override val featureSize: Int,
    /** Candidate splits considered at every audit leaf. Pass an empty list to disable
     *  growth; the regressor then degenerates to a single global accumulator. */
    val splitCandidates: List<SerializableSplit>,
    /** Tunables shared with the underlying [RegressionTree]. */
    val config: RegressionTreeConfig = RegressionTreeConfig(),
    override val concurrency: Concurrency = Concurrency.None,
    /** Leaf-arm factory; defaults to a fresh [VarianceStat] honouring the regressor's [concurrency]. */
    private val leafArmFactory: () -> SeriesStat<WeightedVarianceResult> = { VarianceStat(concurrency) },
    randomSeed: Int = 0,
) : RegressionStat<TreeRegressionResult> {

    init {
        requirePositiveFeatureSize(featureSize)
    }

    private val seedRng = Random(randomSeed)
    private var tree: RegressionTree<VectorView> = newTree()

    private fun newTree(): RegressionTree<VectorView> = RegressionTree(
        splitCandidates,
        config,
        concurrency,
        leafArmFactory,
        seedRng.nextInt(),
    )

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        // Return before touching the tree: a zero-weight call would still advance the leaves'
        // observationsSinceLastCheck and shift the split-audit cadence.
        if (weight.isInertWeight()) return
        tree.update(x, y, weight)
    }

    override fun read(timestampNanos: Long): TreeRegressionResult = TreeRegressionResult(tree.rootNode().snapshot())

    override fun merge(values: TreeRegressionResult) {
        tree.mergeSnapshot(values.root)
    }

    override fun reset() {
        tree = newTree()
    }

    override fun create(concurrency: Concurrency?): DecisionTreeRegressionStat = DecisionTreeRegressionStat(
        featureSize = featureSize,
        splitCandidates = splitCandidates,
        config = config,
        concurrency = concurrency ?: this.concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = seedRng.nextInt(),
    )

    /** Live underlying tree. Use for inspection / pretty-printing. */
    fun tree(): RegressionTree<VectorView> = tree
}

package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult

/**
 * Online VFDT decision-tree regressor — a piecewise-constant predictor over the feature
 * space, growing on the fly via the Hoeffding bound. Wraps a [Tree] in the kumulant
 * [RegressionStat] contract so it composes with everything that consumes regressors
 * (the bandit family, schemas, op pipelines).
 *
 * Snapshots are immutable [TreeRegressionResult]s carrying the frozen split structure
 * and per-node weighted-variance aggregates; bandits pair this with a tree-aware
 * [com.eignex.kumulant.stat.regression.RegressionPosterior] (e.g. `MeanTreePosterior`
 * or `ThompsonTreePosterior`) to score arms at choose time.
 *
 * Reward encoding lives at the call site — pre-transform `y` (e.g. `ln(y)`) before
 * [update]. The internal leaf accumulator is fixed to [VarianceStat]'s
 * [WeightedVarianceResult] so the [VarianceReduction] split metric applies.
 *
 * # Concurrency
 *
 * Leaf-arm updates are lock-free (each arm is a [VarianceStat] that honours
 * [Concurrency]). Split conversion — the only path that mutates tree
 * structure — is serialised by a per-tree lock that fires only once every
 * [TreeConfig.splitPeriod] observations per audit leaf. The hot update path
 * is therefore pure arm arithmetic with zero structural reference writes in
 * the common case. See [Tree] for the full concurrency design.
 */
class DecisionTreeRegressionStat(
    override val featureSize: Int,
    /** Candidate splits considered at every audit leaf. Pass an empty list to disable
     *  growth — the regressor then degenerates to a single global accumulator. */
    val splitCandidates: List<Split>,
    /** Tunables shared with the underlying [Tree]. */
    val config: TreeConfig = TreeConfig(),
    override val concurrency: Concurrency = Concurrency.None,
    /** Leaf-arm factory; defaults to a fresh [VarianceStat] honouring the regressor's [concurrency]. */
    private val leafArmFactory: () -> SeriesStat<WeightedVarianceResult> = { VarianceStat(concurrency) },
    randomSeed: Int = 0,
) : RegressionStat<TreeRegressionResult> {

    init { require(featureSize > 0) { "featureSize must be positive, got $featureSize" } }

    private val seedRng = kotlin.random.Random(randomSeed)
    private var tree: Tree = newTree()

    private fun newTree(): Tree = Tree(splitCandidates, config, concurrency, leafArmFactory, seedRng.nextInt())

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        tree.update(x, y, weight)
    }

    override fun read(timestampNanos: Long): TreeRegressionResult =
        TreeRegressionResult(tree.rootNode().snapshot())

    override fun merge(values: TreeRegressionResult) {
        tree.mergeSnapshot(values.root)
    }

    override fun reset() {
        tree = newTree()
    }

    override fun create(concurrency: Concurrency?): DecisionTreeRegressionStat =
        DecisionTreeRegressionStat(
            featureSize = featureSize,
            splitCandidates = splitCandidates,
            config = config,
            concurrency = concurrency ?: this.concurrency,
            leafArmFactory = leafArmFactory,
            randomSeed = seedRng.nextInt(),
        )

    /** Live underlying tree. Use for inspection / pretty-printing. */
    fun tree(): Tree = tree
}

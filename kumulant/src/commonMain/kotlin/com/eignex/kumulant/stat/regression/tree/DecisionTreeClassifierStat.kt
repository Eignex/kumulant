package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.VectorView
import kotlin.random.Random

/**
 * Online VFDT decision-tree classifier — the classification counterpart of
 * [DecisionTreeRegressionStat]. Each leaf carries a [ClassCountsResult] (per-class
 * weighted counts); splits fire when a candidate beats the runner-up by the
 * Hoeffding bound on Gini reduction or information gain.
 *
 * Input is fed through [RegressionStat]: `x` is the feature vector, `y` is the
 * class index in `[0, numClasses)` (truncated via `toInt()`).
 */
class DecisionTreeClassifierStat(
    override val featureSize: Int,
    /** Number of classes; the input `y` must round to `[0, numClasses)`. */
    val numClasses: Int,
    /** Candidate splits considered at every audit leaf. */
    val splitCandidates: List<Split>,
    /** Tunables shared with the underlying [ClassificationTree]. */
    val config: ClassificationTreeConfig = ClassificationTreeConfig(),
    override val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: () -> SeriesStat<ClassCountsResult> = { ClassCountsStat(numClasses, concurrency) },
    randomSeed: Int = 0,
) : RegressionStat<TreeClassificationResult> {

    init {
        require(featureSize > 0) { "featureSize must be positive, got $featureSize" }
        require(numClasses >= 2) { "numClasses must be >= 2; got $numClasses" }
    }

    private val seedRng = Random(randomSeed)
    private var tree: ClassificationTree = newTree()

    private fun newTree(): ClassificationTree = ClassificationTree(
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        config = config,
        concurrency = concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = seedRng.nextInt(),
    )

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        if (weight <= 0.0 || y.isNaN()) return
        tree.update(x, y.toInt(), weight)
    }

    override fun read(timestampNanos: Long): TreeClassificationResult =
        TreeClassificationResult(tree.rootNode().snapshot())

    override fun merge(values: TreeClassificationResult) {
        require(values.numClasses == numClasses) {
            "merge: numClasses mismatch (${values.numClasses} vs $numClasses)"
        }
        tree.mergeSnapshot(values.root)
    }

    override fun reset() {
        tree = newTree()
    }

    override fun create(concurrency: Concurrency?): DecisionTreeClassifierStat = DecisionTreeClassifierStat(
        featureSize = featureSize,
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        config = config,
        concurrency = concurrency ?: this.concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = seedRng.nextInt(),
    )

    /** Live underlying classification tree. Use for inspection / pretty-printing. */
    fun tree(): ClassificationTree = tree
}

package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireAtLeastTwoClasses
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import kotlin.random.Random

/**
 * Online VFDT decision-tree classifier; the classification counterpart of
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
    val splitCandidates: List<SerializableSplit>,
    /** Tunables shared with the underlying [ClassificationTree]. */
    val config: ClassificationTreeConfig = ClassificationTreeConfig(),
    override val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: () -> SeriesStat<ClassCountsResult> = { ClassCountsStat(numClasses, concurrency) },
    randomSeed: Int = 0,
) : RegressionStat<TreeClassificationResult> {

    init {
        requirePositiveFeatureSize(featureSize)
        requireAtLeastTwoClasses(numClasses)
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
        x.requireFeatureSize(featureSize)
        // Was `weight <= 0.0`, which is false for NaN, and `y.toInt()` with no validation at all - the
        // range check happened downstream in ClassificationTree.update but the truncation did not, so
        // y = 1.5 arrived as a legitimate class 1. A NaN weight got all the way to the leaf counts.
        // isInertWeight, like the regression counterpart: a class count downdates exactly.
        if (weight.isInertWeight()) return
        val c = y.asClassLabel(numClasses)
        if (c < 0) return
        tree.update(x, c, weight)
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

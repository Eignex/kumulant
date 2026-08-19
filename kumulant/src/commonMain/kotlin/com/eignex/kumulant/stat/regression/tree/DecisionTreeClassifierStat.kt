package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.F64VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireAtLeastTwoClasses
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.math.CounterRandom

/**
 * Online VFDT decision-tree classifier; the classification counterpart of
 * [DecisionTreeRegressionStat]. Each leaf carries a [ClassCountsResult] (per-class
 * weighted counts); splits fire when a candidate beats the runner-up by the
 * Hoeffding bound on Gini reduction or information gain.
 *
 * Input is fed through [RegressionStat]: `x` is the feature vector, `y` is the
 * class index in `[0, numClasses)`, resolved by [asClassLabel].
 *
 * **Use cases:** online classification where the decision boundary is
 * axis-aligned and piecewise constant, and the model must grow with the stream
 * rather than be refit. Reach for [RandomForestClassifierStat] for ensembled
 * diversity, or [DecisionTreeRegressionStat] when the target is continuous.
 *
 * **Memory:** O(nodes · [splitCandidates]); a [ClassCountsStat] per node plus
 * per-audit-leaf candidate accumulators. Bounded by
 * [ClassificationTreeConfig.maxNodes].
 *
 * **Update:** O(depth) per observation; a tree walk to the destination leaf,
 * then a leaf-arm update. Splits fire at most once every
 * [ClassificationTreeConfig.splitPeriod] observations per audit leaf.
 *
 * **Concurrency:** Inherits [DecisionTreeRegressionStat]'s concurrency model,
 * with [ClassCountsStat] leaf arms in place of variance accumulators. The update path
 * touches exactly one leaf, so threads landing in different leaves never
 * contend; split conversion takes a per-tree lock fired only at split
 * decisions. See [ClassificationTree] for the full design.
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
    private val leafArmFactory: (() -> SeriesStat<ClassCountsResult>)? = null,
    randomSeed: Int = 0,
) : RegressionStat<TreeClassificationResult> {

    init {
        requirePositiveFeatureSize(featureSize)
        requireAtLeastTwoClasses(numClasses)
    }

    private val seedRng = CounterRandom(randomSeed.toLong(), concurrency)

    // Resolved per instance, not captured in the constructor default: create() has to be able
    // to rebind the default arm to the replica's concurrency.
    private val armFactory: () -> SeriesStat<ClassCountsResult> = leafArmFactory ?: {
        ClassCountsStat(
            numClasses,
            concurrency,
        )
    }
    private var tree: ClassificationTree = newTree()

    private fun newTree(): ClassificationTree = ClassificationTree(
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        config = config,
        concurrency = concurrency,
        leafArmFactory = armFactory,
        randomSeed = seedRng.nextInt(),
    )

    override fun update(x: F64VectorView, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        // isInertWeight rather than `weight <= 0.0`, which is false for NaN: a class count downdates
        // exactly, so a negative weight is a real retraction while a NaN would pin a count for good.
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

    /**
     * Restarts the seed stream too, so the rebuilt tree is the one a fresh stat would have grown.
     * Without it a reset stat draws its next seed from wherever construction left off, and `reset`
     * promises the equivalent of a fresh stat rather than merely an empty one.
     */
    override fun reset() {
        seedRng.reset()
        tree = newTree()
    }

    override fun create(concurrency: Concurrency?): DecisionTreeClassifierStat = DecisionTreeClassifierStat(
        featureSize = featureSize,
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        config = config,
        concurrency = concurrency ?: this.concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = seedRng.childSeed().toInt(),
    )

    /** Live underlying classification tree. Use for inspection / pretty-printing. */
    fun tree(): ClassificationTree = tree
}

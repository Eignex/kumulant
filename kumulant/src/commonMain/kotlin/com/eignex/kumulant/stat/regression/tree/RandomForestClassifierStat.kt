package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireAtLeastTwoClasses
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.math.CounterRandom
import com.eignex.kumulant.math.argMaxOf
import com.eignex.kumulant.math.nextPoissonOne
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
    val nbrTrees: Int = 10,
    config: ClassificationTreeConfig = ClassificationTreeConfig(),
    /** Oza & Russell online bagging: per-tree Poisson(1) reweighting at update time. */
    val bagging: Boolean = true,
    override val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: (() -> SeriesStat<ClassCountsResult>)? = null,
    randomSeed: Int = 0,
) : RegressionStat<ForestClassificationResult> {

    init {
        requirePositiveFeatureSize(featureSize)
        requireAtLeastTwoClasses(numClasses)
        require(nbrTrees > 0) { "nbrTrees must be positive, got $nbrTrees" }
    }

    /** [ClassificationTreeConfig] with [ClassificationTreeConfig.mtry] defaulted to `ceil(sqrt(p))` when null. */
    val config: ClassificationTreeConfig = config.copy(mtry = config.mtry ?: defaultMtry(splitCandidates.size))

    private val seedRng = CounterRandom(randomSeed.toLong(), concurrency)

    // Resolved per instance, not captured in the constructor default: create() has to be able
    // to rebind the default arm to the replica's concurrency.
    private val armFactory: () -> SeriesStat<ClassCountsResult> = leafArmFactory ?: {
        ClassCountsStat(
            numClasses,
            concurrency,
        )
    }

    // Drawn from on the update path, once per tree, with no lock over it - so it has to be a generator
    // that concurrent updates can share. See CounterRandom.
    private val baggingRng = CounterRandom(seedRng.childSeed(), concurrency)
    private var trees: Array<ClassificationTree> = Array(nbrTrees) { newTree() }

    private fun newTree(): ClassificationTree = ClassificationTree(
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        config = this.config,
        concurrency = concurrency,
        leafArmFactory = armFactory,
        randomSeed = seedRng.nextInt(),
    )

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        // isInertWeight rather than isNotPositiveWeight: a class count subtracts exactly, so a negative
        // weight is a real downdate here, exactly as it is for the regression forest. Returning before
        // the bagging draw also matters - an inert call that consumed one Poisson draw per tree would
        // desynchronise every later one.
        if (weight.isInertWeight()) return
        val c = y.asClassLabel(numClasses)
        if (c < 0) return
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

    override fun create(concurrency: Concurrency?): RandomForestClassifierStat = RandomForestClassifierStat(
        featureSize = featureSize,
        numClasses = numClasses,
        splitCandidates = splitCandidates,
        nbrTrees = nbrTrees,
        config = config,
        bagging = bagging,
        concurrency = concurrency ?: this.concurrency,
        leafArmFactory = leafArmFactory,
        randomSeed = seedRng.childSeed().toInt(),
    )

    /** Live underlying classification trees. Use for inspection. */
    fun trees(): List<ClassificationTree> = trees.toList()
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
        return argMaxOf(numClasses) { k -> p[k] }
    }

    /** Sum of per-tree root totalWeights. */
    val totalWeights: Double get() = trees.sumOf { it.totalWeights }
}

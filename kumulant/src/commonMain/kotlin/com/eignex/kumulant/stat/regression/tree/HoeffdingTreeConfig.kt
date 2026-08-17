package com.eignex.kumulant.stat.regression.tree

/**
 * Growth tunables common to both VFDT trees.
 *
 * [RegressionTreeConfig] and [ClassificationTreeConfig] were field-for-field identical except for the
 * split criterion, and their KDoc had already drifted apart - the classification copy lost the
 * explanations of what [tau] and [mtry] actually do. They stay separate types because each is
 * `@Serializable` and part of the wire format, and because [RegressionTreeConfig.metric] and
 * [ClassificationTreeConfig.metric] have different types; this interface is where the shared half is
 * described once, and it is what lets the growth logic read tunables without knowing which tree it is
 * driving.
 */
interface HoeffdingTreeConfig {

    /**
     * Hoeffding-bound confidence threshold. Lower means splits require more evidence.
     *
     * This is the `delta` of the bound `sqrt(-ln(delta) / 2n)`, so it enters logarithmically: an order
     * of magnitude buys only a modest widening of the margin a candidate has to clear.
     */
    val delta: Double

    /**
     * Multiplicative decay applied to [delta] per depth, which slows growth near the leaves.
     *
     * Deeper leaves see less of the stream, so a fixed confidence level would let them split on
     * proportionally thinner evidence. Shrinking delta with depth counteracts that.
     */
    val deltaDecay: Double

    /**
     * If the Hoeffding bound itself shrinks below this, the leaf may split even when the runner-up is
     * close: the classic VFDT tie-break parameter.
     *
     * Without it, two candidates of genuinely equal merit deadlock forever, because the margin between
     * them never exceeds any bound. This gives the tree permission to pick one once it has enough
     * evidence to know the choice does not matter much.
     */
    val tau: Double

    /** Minimum total weighted samples at a leaf before split evaluation runs at all. */
    val minSamplesSplit: Double

    /** Minimum weighted samples required on each side of a candidate split. */
    val minSamplesLeaf: Double

    /**
     * Audit every Nth observation rather than every update.
     *
     * Evaluating every candidate at every observation is the dominant cost in a VFDT, and the bound
     * moves slowly, so checking periodically loses almost nothing.
     */
    val splitPeriod: Int

    /** Hard ceiling on tree depth. */
    val maxDepth: Int

    /** Hard ceiling on internal plus leaf nodes. */
    val maxNodes: Int

    /**
     * Breiman-style random-subspace size: at every audit-leaf birth, draw a fresh random subset of this
     * many candidates from the tree's full pool. `null` disables the trick and considers every candidate.
     *
     * This is what decorrelates the trees in a forest. A single tree usually leaves it null.
     */
    val mtry: Int?
}

/**
 * The growth defaults both VFDT trees use.
 *
 * [HoeffdingTreeConfig] is an interface, so it can describe these fields but cannot carry their values -
 * and so [RegressionTreeConfig] and [ClassificationTreeConfig] each declared all nine numbers. A tuning
 * change to one tree silently left the other behind, and `TreeInternalsTest` already asserts that
 * `shouldSplit` agrees between the two default configs, which means the test suite depended on that
 * duplication being kept in sync by hand.
 *
 * Referencing a `const` from a constructor default is not an ABI change: the value is compiled into the
 * synthetic defaults method exactly as the literal was.
 */
internal object HoeffdingDefaults {
    /** See [HoeffdingTreeConfig.delta]. */
    const val DELTA: Double = 0.05

    /** See [HoeffdingTreeConfig.deltaDecay]. */
    const val DELTA_DECAY: Double = 0.9

    /** See [HoeffdingTreeConfig.tau]. */
    const val TAU: Double = 0.05

    /** See [HoeffdingTreeConfig.minSamplesSplit]. */
    const val MIN_SAMPLES_SPLIT: Double = 30.0

    /** See [HoeffdingTreeConfig.minSamplesLeaf]. */
    const val MIN_SAMPLES_LEAF: Double = 5.0

    /** See [HoeffdingTreeConfig.splitPeriod]. */
    const val SPLIT_PERIOD: Int = 10

    /** See [HoeffdingTreeConfig.maxDepth]. */
    const val MAX_DEPTH: Int = 16

    /** See [HoeffdingTreeConfig.maxNodes]. */
    const val MAX_NODES: Int = 1024
}

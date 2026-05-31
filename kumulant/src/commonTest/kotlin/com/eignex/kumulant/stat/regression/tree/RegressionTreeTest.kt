package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.math.DenseVector
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RegressionTreeTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    private fun newTree(
        candidates: List<SerializableSplit> = emptyList(),
        config: RegressionTreeConfig = RegressionTreeConfig(),
    ) = RegressionTree(splitCandidates = candidates, config = config, randomSeed = 0)

    @Test
    fun `update folds into the root arm even without splits`() {
        val tree = newTree()
        tree.update(feat(0.0), 5.0, 1.0)
        tree.update(feat(0.0), 7.0, 1.0)
        val snap = tree.rootSnapshot()
        assertEquals(2.0, snap.totalWeights, 1e-9)
        assertEquals(6.0, snap.mean, 1e-9)
    }

    @Test
    fun `nodeCount starts at 1 and grows as splits trigger`() {
        val tree = newTree(
            candidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
        )
        assertEquals(1, tree.nodeCount)
        repeat(20) {
            val x = if (it % 2 == 0) -1.0 else 1.0
            tree.update(feat(x), x, 1.0)
        }
        assertTrue(tree.nodeCount >= 3, "expected at least 3 nodes after splitting, got ${tree.nodeCount}")
    }

    @Test
    fun `reset returns to a single leaf`() {
        val tree = newTree(
            candidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
        )
        repeat(20) { tree.update(feat(if (it % 2 == 0) -1.0 else 1.0), 1.0) }
        tree.reset()
        assertEquals(1, tree.nodeCount)
        assertEquals(0.0, tree.rootSnapshot().totalWeights, 1e-9)
    }

    @Test
    fun `growth halts at maxNodes`() {
        val tree = newTree(
            candidates = listOf(ThresholdSplit(0, 0.0), ThresholdSplit(0, 0.5), ThresholdSplit(0, -0.5)),
            config = RegressionTreeConfig(splitPeriod = 2, minSamplesSplit = 2.0, minSamplesLeaf = 1.0, maxNodes = 3),
        )
        repeat(40) { tree.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        assertTrue(tree.nodeCount <= 3, "expected <= 3 nodes (maxNodes cap), got ${tree.nodeCount}")
    }

    @Test
    fun `predict equals leaf mean at the routed leaf`() {
        val tree = newTree()
        tree.update(feat(0.0), 3.0)
        tree.update(feat(0.0), 5.0)
        assertEquals(4.0, tree.predict(feat(0.0)), 1e-9)
    }

    @Test
    fun `prettyPrint emits leaf mean when no splits`() {
        val tree = newTree()
        tree.update(feat(0.0), 2.0)
        // JS renders Double 2.0 without a trailing `.0`; accept either.
        val out = tree.prettyPrint()
        assertTrue(out.contains("leaf mean=2"), "expected leaf mean in:\n$out")
    }

    @Test
    fun `prettyPrint emits split predicate after growth`() {
        val tree = newTree(
            candidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
        )
        repeat(20) { tree.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        val out = tree.prettyPrint()
        // JS may render Double 0.0 as "0"; accept either.
        assertTrue(
            out.contains("if (x[0] <= 0.0)") || out.contains("if (x[0] <= 0)"),
            "expected split predicate in:\n$out",
        )
    }

    @Test
    fun `merge structurally combines same-split trees`() {
        val cfg = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0)
        val splits = listOf(ThresholdSplit(0, 0.0))
        val a = RegressionTree(splits, cfg, randomSeed = 1)
        val b = RegressionTree(splits, cfg, randomSeed = 2)
        repeat(20) {
            val x = if (it % 2 == 0) -1.0 else 1.0
            a.update(feat(x), x)
            b.update(feat(x), x)
        }
        val aBefore = a.rootSnapshot().totalWeights
        a.merge(b)
        assertTrue(a.rootSnapshot().totalWeights > aBefore)
    }

    @Test
    fun `mergeSnapshot adopts other structure when self is a leaf`() {
        val cfg = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0)
        val splits = listOf(ThresholdSplit(0, 0.0))
        val grown = RegressionTree(splits, cfg, randomSeed = 3)
        repeat(20) { grown.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        val fresh = RegressionTree(splits, cfg, randomSeed = 4)
        assertEquals(1, fresh.nodeCount)
        fresh.mergeSnapshot(grown.rootNode().snapshot())
        assertTrue(fresh.nodeCount >= 3, "fresh should adopt grown structure, got ${fresh.nodeCount}")
    }
}

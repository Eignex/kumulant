package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.DenseVector
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TreeClassificationResultTest {

    private fun counts(vararg c: Double) = ClassCountsResult(c.size, c)
    private fun vec(vararg xs: Double) = DenseVector.of(xs)

    private fun stubTree(): TreeClassificationResult {
        // x[0] <= 0 routes pos (class 0 dominant); otherwise neg (class 1 dominant)
        val pos = TreeClassificationLeafResult(counts(8.0, 2.0))
        val neg = TreeClassificationLeafResult(counts(1.0, 9.0))
        val split = TreeClassificationSplitResult(
            split = ThresholdSplit(0, 0.0),
            pos = pos,
            neg = neg,
            value = mergeCC(counts(8.0, 2.0), counts(1.0, 9.0)),
        )
        return TreeClassificationResult(split)
    }

    @Test
    fun `leaf result routes to its own value`() {
        val leaf = TreeClassificationLeafResult(counts(3.0, 5.0))
        assertEquals(leaf.value, leaf.findLeaf(vec(0.0)))
    }

    @Test
    fun `split result routes by predicate`() {
        val tree = stubTree()
        assertEquals(counts(8.0, 2.0), tree.findLeaf(vec(-1.0)))
        assertEquals(counts(1.0, 9.0), tree.findLeaf(vec(1.0)))
    }

    @Test
    fun `predict returns argmax at the routed leaf`() {
        val tree = stubTree()
        assertEquals(0, tree.predict(vec(-1.0)))
        assertEquals(1, tree.predict(vec(1.0)))
    }

    @Test
    fun `probabilities at routed leaf reflect leaf counts`() {
        val tree = stubTree()
        val p = tree.probabilities(vec(-1.0))
        assertEquals(1.0, p.sum(), 1e-12)
        assertEquals(0.8, p[0], 1e-12)
        assertEquals(0.2, p[1], 1e-12)
    }

    @Test
    fun `totalWeights reflects root aggregate across both children`() {
        val tree = stubTree()
        assertEquals(20.0, tree.totalWeights, 1e-12)
        assertEquals(2, tree.numClasses)
    }

    @Test
    fun `Forest probabilities average per-tree probabilities`() {
        val t1 = TreeClassificationResult(TreeClassificationLeafResult(counts(4.0, 0.0))) // [1, 0]
        val t2 = TreeClassificationResult(TreeClassificationLeafResult(counts(0.0, 2.0))) // [0, 1]
        val forest = ForestClassificationResult(numClasses = 2, trees = listOf(t1, t2))
        val p = forest.probabilities(vec(0.0))
        assertEquals(0.5, p[0], 1e-12)
        assertEquals(0.5, p[1], 1e-12)
    }

    @Test
    fun `Forest predict picks the higher averaged probability`() {
        val t1 = TreeClassificationResult(TreeClassificationLeafResult(counts(3.0, 1.0))) // [0.75, 0.25]
        val t2 = TreeClassificationResult(TreeClassificationLeafResult(counts(2.0, 2.0))) // [0.5, 0.5]
        val forest = ForestClassificationResult(numClasses = 2, trees = listOf(t1, t2))
        assertEquals(0, forest.predict(vec(0.0)))
    }

    @Test
    fun `Forest totalWeights sums per-tree totals`() {
        val t1 = TreeClassificationResult(TreeClassificationLeafResult(counts(3.0, 1.0)))
        val t2 = TreeClassificationResult(TreeClassificationLeafResult(counts(2.0, 5.0)))
        val forest = ForestClassificationResult(numClasses = 2, trees = listOf(t1, t2))
        assertEquals(11.0, forest.totalWeights, 1e-12)
    }

    @Test
    fun `Forest rejects empty trees`() {
        assertFailsWith<IllegalArgumentException> {
            ForestClassificationResult(numClasses = 2, trees = emptyList())
        }
    }

    @Test
    fun `Forest rejects mismatched numClasses`() {
        val t1 = TreeClassificationResult(TreeClassificationLeafResult(counts(1.0, 1.0)))
        val t2 = TreeClassificationResult(TreeClassificationLeafResult(counts(1.0, 1.0, 1.0)))
        assertFailsWith<IllegalArgumentException> {
            ForestClassificationResult(numClasses = 2, trees = listOf(t1, t2))
        }
    }

    @Test
    fun `empty-leaf probabilities fall back to uniform`() {
        val tree = TreeClassificationResult(TreeClassificationLeafResult(counts(0.0, 0.0, 0.0)))
        val p = tree.probabilities(vec(0.0))
        for (pk in p) assertTrue(pk in 0.32..0.34, "expected ~1/3, got $pk")
        assertEquals(1.0, p.sum(), 1e-12)
    }
}

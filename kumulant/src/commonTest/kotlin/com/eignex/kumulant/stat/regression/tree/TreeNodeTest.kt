package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.DenseVector
import com.eignex.koblas.Vector
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

class TreeNodeTest {

    private fun wvLeaf(): RegressionTerminalLeaf<Vector> = RegressionTerminalLeaf(VarianceStat())
    private fun ccLeaf(numClasses: Int = 2) = ClassificationTerminalLeaf(ClassCountsStat(numClasses))

    @Test
    fun `RegressionSplitNode findLeaf routes by predicate`() {
        val pos = wvLeaf()
        val neg = wvLeaf()
        val node = RegressionSplitNode(ThresholdSplit(0, 0.0), pos = pos, neg = neg)
        assertSame(pos, node.findLeaf(DenseVector.of(doubleArrayOf(-1.0))))
        assertSame(neg, node.findLeaf(DenseVector.of(doubleArrayOf(1.0))))
        // Threshold is inclusive on the pos side.
        assertSame(pos, node.findLeaf(DenseVector.of(doubleArrayOf(0.0))))
    }

    @Test
    fun `RegressionAuditLeaf requires aligned candidate sizes`() {
        val arm = VarianceStat()
        val candidates = listOf<SerializableSplit>(ThresholdSplit(0, 0.0))
        assertFailsWith<IllegalArgumentException> {
            RegressionAuditLeaf(
                arm = arm,
                candidates = candidates,
                pos = listOf(VarianceStat(), VarianceStat()),
                neg = listOf(VarianceStat()),
            )
        }
    }

    @Test
    fun `ClassificationSplitNode findLeaf routes by predicate`() {
        val pos = ccLeaf()
        val neg = ccLeaf()
        val node = ClassificationSplitNode(ThresholdSplit(0, 0.5), pos = pos, neg = neg)
        assertSame(pos, node.findLeaf(DenseVector.of(doubleArrayOf(0.0))))
        assertSame(neg, node.findLeaf(DenseVector.of(doubleArrayOf(1.0))))
    }

    @Test
    fun `ClassificationAuditLeaf requires aligned candidate sizes`() {
        assertFailsWith<IllegalArgumentException> {
            ClassificationAuditLeaf(
                arm = ClassCountsStat(2),
                candidates = listOf(ThresholdSplit(0, 0.0)),
                pos = listOf(ClassCountsStat(2)),
                neg = emptyList(),
            )
        }
    }
}

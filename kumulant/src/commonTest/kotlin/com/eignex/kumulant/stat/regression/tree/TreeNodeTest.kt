@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

class TreeNodeTest {

    private fun wvLeaf(): RegressionTerminalLeaf<F64VectorLike> = RegressionTerminalLeaf(VarianceStat())
    private fun ccLeaf(numClasses: Int = 2) = ClassificationTerminalLeaf(ClassCountsStat(numClasses))

    @Test
    fun `RegressionSplitNode findLeaf routes by predicate`() {
        val pos = wvLeaf()
        val neg = wvLeaf()
        val node = RegressionSplitNode(ThresholdSplit(0, 0.0), pos = pos, neg = neg)
        assertSame(pos, node.findLeaf(F64DenseVector.of(doubleArrayOf(-1.0))))
        assertSame(neg, node.findLeaf(F64DenseVector.of(doubleArrayOf(1.0))))
        // Threshold is inclusive on the pos side.
        assertSame(pos, node.findLeaf(F64DenseVector.of(doubleArrayOf(0.0))))
    }

    @Test
    fun `RegressionSplitNode children are volatile and reassignable`() {
        val original = wvLeaf()
        val replacement = wvLeaf()
        val node = RegressionSplitNode(ThresholdSplit(0, 0.0), pos = original, neg = wvLeaf())
        assertSame(original, node.pos)
        node.pos = replacement
        assertSame(replacement, node.findLeaf(F64DenseVector.of(doubleArrayOf(-1.0))))
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
    fun `RegressionAuditLeaf starts with zero audit ticks`() {
        val leaf = RegressionAuditLeaf(
            arm = VarianceStat(),
            candidates = listOf(ThresholdSplit(0, 0.0)),
            pos = listOf(VarianceStat()),
            neg = listOf(VarianceStat()),
        )
        assertEquals(0L, leaf.observationsSinceLastCheck.load())
    }

    @Test
    fun `ClassificationSplitNode findLeaf routes by predicate`() {
        val pos = ccLeaf()
        val neg = ccLeaf()
        val node = ClassificationSplitNode(ThresholdSplit(0, 0.5), pos = pos, neg = neg)
        assertSame(pos, node.findLeaf(F64DenseVector.of(doubleArrayOf(0.0))))
        assertSame(neg, node.findLeaf(F64DenseVector.of(doubleArrayOf(1.0))))
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

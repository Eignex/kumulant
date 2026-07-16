package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TreeRegressionResultTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    @Test
    fun `findLeaf returns the leaf value for a flat tree`() {
        val leafVal = WeightedVarianceResult(5.0, 2.0, 0.5)
        val r = TreeRegressionResult(TreeLeafResult(leafVal))
        assertEquals(leafVal, r.findLeaf(feat(0.0)))
    }

    @Test
    fun `findLeaf routes through nested splits`() {
        // RegressionTree:        x[0] <= 0
        //              /        \
        //         x[1] <= 0     leaf C (value=20)
        //         /        \
        //   leaf A (val=1) leaf B (val=2)
        val a = WeightedVarianceResult(1.0, 1.0, 0.0)
        val b = WeightedVarianceResult(1.0, 2.0, 0.0)
        val c = WeightedVarianceResult(1.0, 20.0, 0.0)
        val r = TreeRegressionResult(
            TreeSplitResult(
                split = ThresholdSplit(0, 0.0),
                pos = TreeSplitResult(
                    split = ThresholdSplit(1, 0.0),
                    pos = TreeLeafResult(a),
                    neg = TreeLeafResult(b),
                    value = WeightedVarianceResult(2.0, 1.5, 0.25),
                ),
                neg = TreeLeafResult(c),
                value = WeightedVarianceResult(3.0, 7.0, 80.0),
            ),
        )
        assertEquals(a, r.findLeaf(feat(-1.0, -1.0)))
        assertEquals(b, r.findLeaf(feat(-1.0, 1.0)))
        assertEquals(c, r.findLeaf(feat(1.0, 0.0)))
    }

    @Test
    fun `predict and rootMean expose scalar summaries`() {
        val r = TreeRegressionResult(TreeLeafResult(WeightedVarianceResult(4.0, 3.0, 0.0)))
        assertEquals(3.0, r.predict(feat(0.0)))
        assertEquals(3.0, r.rootMean)
        assertEquals(4.0, r.totalWeights)
    }

    @Test
    fun `RegressionNode snapshot freezes structure`() {
        // Verify snapshot round-trip preserves split predicates and leaf values.
        val live = RegressionTree(
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 0,
        )
        repeat(20) { live.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        val frozen = live.rootNode().snapshot()
        assertTrue(frozen is TreeSplitResult, "expected split after growth, got $frozen")
        assertEquals(ThresholdSplit(0, 0.0), frozen.split)
    }
}

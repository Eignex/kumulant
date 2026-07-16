package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.core.Concurrency
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class DecisionTreeRegressionStatTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    @Test
    fun `rejects bad featureSize`() {
        assertFailsWith<IllegalArgumentException> {
            DecisionTreeRegressionStat(featureSize = 0, splitCandidates = emptyList())
        }
    }

    @Test
    fun `predict reflects context routing after splits grow`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
            randomSeed = 1,
        )
        val rng = Random(1)
        repeat(200) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), if (x > 0) 1.0 else -1.0)
        }
        val snap = stat.read(0L)
        assertTrue(snap.predict(feat(0.5)) > snap.predict(feat(-0.5)))
    }

    @Test
    fun `merge folds snapshot into stat`() {
        val a = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 2)
        val b = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 3)
        repeat(20) {
            a.update(feat(0.0), 1.0)
            b.update(feat(0.0), 3.0)
        }
        val aBefore = a.read(0L).totalWeights
        a.merge(b.read(0L))
        assertTrue(a.read(0L).totalWeights > aBefore)
    }

    @Test
    fun `reset returns to a single leaf`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 4,
        )
        repeat(100) { stat.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        assertTrue(stat.tree().nodeCount >= 3)
        stat.reset()
        assertEquals(1, stat.tree().nodeCount)
    }

    @Test
    fun `empty splitCandidates degenerates to a single leaf`() {
        val stat = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 40)
        repeat(500) { stat.update(feat(it.toDouble()), it.toDouble()) }
        assertEquals(1, stat.tree().nodeCount)
        val snap = stat.read(0L)
        assertEquals(snap.predict(feat(-100.0)), snap.predict(feat(100.0)))
    }

    @Test
    fun `maxNodes caps growth`() {
        val candidates = (0 until 8).map { ThresholdSplit(0, it * 0.2 - 0.8) }
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = candidates,
            config = RegressionTreeConfig(
                splitPeriod = 4,
                minSamplesSplit = 4.0,
                minSamplesLeaf = 1.0,
                maxNodes = 5,
                tau = 1.0,
            ),
            randomSeed = 41,
        )
        val rng = Random(41)
        repeat(2000) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), x)
        }
        assertTrue(stat.tree().nodeCount <= 5, "nodeCount=${stat.tree().nodeCount}")
    }

    @Test
    fun `maxDepth caps growth`() {
        val candidates = (0 until 8).map { ThresholdSplit(0, it * 0.2 - 0.8) }
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = candidates,
            config = RegressionTreeConfig(
                splitPeriod = 4,
                minSamplesSplit = 4.0,
                minSamplesLeaf = 1.0,
                maxDepth = 2,
                tau = 1.0,
            ),
            randomSeed = 42,
        )
        val rng = Random(42)
        repeat(2000) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), x)
        }
        assertTrue(stat.tree().nodeCount <= 7, "nodeCount=${stat.tree().nodeCount}")
    }

    @Test
    fun `prettyPrint renders split structure`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 50,
        )
        repeat(200) { stat.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        val rendered = stat.tree().prettyPrint()
        assertTrue("x[0]" in rendered, "expected split predicate, got:\n$rendered")
        assertTrue("leaf mean=" in rendered)
        assertTrue("} else {" in rendered)
    }

    @Test
    fun `snapshot routes to same leaf as live tree`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 60,
        )
        val rng = Random(60)
        repeat(300) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), x)
        }
        val snap = stat.read(0L)
        for (x in listOf(-0.9, -0.1, 0.1, 0.9)) {
            assertEquals(stat.tree().predict(feat(x)), snap.predict(feat(x)), 1e-12)
        }
    }

    @Test
    fun `Concurrency Relaxed preserves total weight on serial updates`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
            concurrency = Concurrency.Relaxed,
            randomSeed = 70,
        )
        var expected = 0.0
        val rng = Random(70)
        repeat(500) {
            val w = 1.0 + rng.nextDouble()
            stat.update(feat(rng.nextDouble() * 2 - 1), rng.nextDouble(), weight = w)
            expected += w
        }
        assertEquals(expected, stat.read(0L).totalWeights, 1e-9)
    }

    @Test
    fun `Concurrency Strict preserves total weight on serial updates`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
            concurrency = Concurrency.Strict,
            randomSeed = 71,
        )
        var expected = 0.0
        val rng = Random(71)
        repeat(500) {
            val w = 1.0 + rng.nextDouble()
            stat.update(feat(rng.nextDouble() * 2 - 1), rng.nextDouble(), weight = w)
            expected += w
        }
        assertEquals(expected, stat.read(0L).totalWeights, 1e-9)
    }
}

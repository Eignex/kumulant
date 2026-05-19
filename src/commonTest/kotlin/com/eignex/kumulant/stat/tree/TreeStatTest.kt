package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.math.DenseVector
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TreeStatTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    @Test
    fun `DecisionTreeRegressionStat rejects bad featureSize`() {
        assertFailsWith<IllegalArgumentException> {
            DecisionTreeRegressionStat(featureSize = 0, splitCandidates = emptyList())
        }
    }

    @Test
    fun `DecisionTreeRegressionStat predict reflects context routing after splits grow`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = TreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
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
            config = TreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 4,
        )
        repeat(100) { stat.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        assertTrue(stat.tree().nodeCount >= 3)
        stat.reset()
        assertEquals(1, stat.tree().nodeCount)
    }

    @Test
    fun `RandomForestRegressionStat predict averages across trees`() {
        val stat = RandomForestRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            nbrTrees = 4,
            config = TreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 5,
        )
        val rng = Random(5)
        repeat(300) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), if (x > 0) 1.0 else -1.0)
        }
        val snap = stat.read(0L)
        assertTrue(snap.predict(feat(0.5)) > snap.predict(feat(-0.5)))
        assertEquals(4, snap.trees.size)
    }

    @Test
    fun `RandomForestRegressionStat merge requires matching forest size`() {
        val a = RandomForestRegressionStat(1, emptyList(), nbrTrees = 3, randomSeed = 6)
        val b = RandomForestRegressionStat(1, emptyList(), nbrTrees = 5, randomSeed = 7)
        assertFailsWith<IllegalArgumentException> { a.merge(b.read(0L)) }
    }

    @Test
    fun `MeanTreePosterior is deterministic`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = emptyList(),
            randomSeed = 30,
        )
        repeat(50) { stat.update(feat(0.0), 4.0) }
        val snap = stat.read(0L)
        val score1 = MeanTreePosterior.evaluate(snap, feat(0.0), Random(0), 1.0)
        val score2 = MeanTreePosterior.evaluate(snap, feat(0.0), Random(123), 0.5)
        assertEquals(score1, score2)
        assertTrue(score1 in 3.9..4.1)
    }

    // === Structural ============================================================

    @Test
    fun `empty splitCandidates degenerates to a single leaf`() {
        val stat = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 40)
        repeat(500) { stat.update(feat(it.toDouble()), it.toDouble()) }
        assertEquals(1, stat.tree().nodeCount)
        val snap = stat.read(0L)
        // Predictions ignore context — same value everywhere.
        assertEquals(snap.predict(feat(-100.0)), snap.predict(feat(100.0)))
    }

    @Test
    fun `maxNodes caps growth`() {
        val candidates = (0 until 8).map { ThresholdSplit(0, it * 0.2 - 0.8) }
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = candidates,
            config = TreeConfig(
                splitPeriod = 4,
                minSamplesSplit = 4.0,
                minSamplesLeaf = 1.0,
                maxNodes = 5,
                tau = 1.0, // force splits via the tie-breaker
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
            config = TreeConfig(
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
        // At depth 2 the tree is at most a root + 2 splits + 4 leaves = 7 nodes.
        assertTrue(stat.tree().nodeCount <= 7, "nodeCount=${stat.tree().nodeCount}")
    }

    @Test
    fun `prettyPrint renders split structure`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = TreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
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
            config = TreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
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

    // === Concurrency-mode plumbing =============================================

    @Test
    fun `Concurrency Relaxed preserves total weight on serial updates`() {
        // Hot-path arithmetic should still be exact when run from a single thread.
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = TreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
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
            config = TreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
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

    // === Posteriors ============================================================

    @Test
    fun `UcbTreePosterior bonus shrinks as evidence grows`() {
        val stat = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 80)
        val rng = Random(80)
        // Noisy rewards around mean 2.0 — variance must be non-zero for the bonus
        // to be observable.
        repeat(5) { stat.update(feat(0.0), 2.0 + rng.nextDouble() - 0.5) }
        val snapEarly = stat.read(0L)
        repeat(5_000) { stat.update(feat(0.0), 2.0 + rng.nextDouble() - 0.5) }
        val snapLate = stat.read(0L)

        val posterior = UcbTreePosterior(priorWeight = 1.0, priorVariance = 1.0)
        val earlyBonus = posterior.evaluate(snapEarly, feat(0.0), Random(0), 1.0) - snapEarly.predict(feat(0.0))
        val lateBonus = posterior.evaluate(snapLate, feat(0.0), Random(0), 1.0) - snapLate.predict(feat(0.0))
        assertTrue(earlyBonus > lateBonus, "earlyBonus=$earlyBonus lateBonus=$lateBonus should shrink")
    }

    @Test
    fun `MeanForestPosterior matches findLeafMerged mean`() {
        val stat = RandomForestRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            nbrTrees = 3,
            config = TreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 90,
        )
        val rng = Random(90)
        repeat(300) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), if (x > 0) 1.0 else -1.0)
        }
        val snap = stat.read(0L)
        val direct = snap.findLeafMerged(feat(0.5)).mean
        val viaPosterior = MeanForestPosterior.evaluate(snap, feat(0.5), Random(0), 1.0)
        assertEquals(direct, viaPosterior, 1e-12)
    }

    @Test
    fun `forest snapshot totalWeights sums per-tree weights under bagging`() {
        val stat = RandomForestRegressionStat(
            featureSize = 1,
            splitCandidates = emptyList(),
            nbrTrees = 5,
            bagging = true,
            randomSeed = 100,
        )
        repeat(1000) { stat.update(feat(0.0), 1.0) }
        val snap = stat.read(0L)
        // Per-tree totals fluctuate around the underlying 1000 due to Poisson(1)
        // bagging; the sum is ~ nbrTrees * 1000.
        assertTrue(snap.totalWeights > 4_000.0, "totalWeights=${snap.totalWeights}")
        assertTrue(snap.totalWeights < 6_000.0, "totalWeights=${snap.totalWeights}")
    }

    @Test
    fun `forest without bagging gives identical per-tree totalWeights`() {
        val stat = RandomForestRegressionStat(
            featureSize = 1,
            splitCandidates = emptyList(),
            nbrTrees = 3,
            bagging = false,
            randomSeed = 101,
        )
        repeat(200) { stat.update(feat(0.0), 1.0) }
        val snap = stat.read(0L)
        val totals = snap.trees.map { it.totalWeights }
        for (t in totals) assertEquals(200.0, t, 1e-9)
    }
}

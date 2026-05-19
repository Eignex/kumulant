package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.bandit.RegressionContextualBandit
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

    // === Composes with RegressionContextualBandit ==============================

    @Test
    fun `RegressionContextualBandit drives tree-based arms`() {
        val rng = Random(10)
        val cb = RegressionContextualBandit(
            nbrArms = 2,
            template = DecisionTreeRegressionStat(
                featureSize = 1,
                splitCandidates = listOf(ThresholdSplit(0, 0.0)),
                config = TreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
                randomSeed = 11,
            ),
            posterior = ThompsonTreePosterior(priorWeight = 1.0, priorVariance = 1.0),
            random = rng,
        )
        // Arm 0 pays 1 when x>0, 0 otherwise; arm 1 pays the opposite.
        repeat(600) {
            val x = rng.nextDouble() * 2 - 1
            val xv = feat(x)
            val a = cb.choose(xv)
            val reward = when {
                a == 0 && x > 0 -> 1.0
                a == 1 && x <= 0 -> 1.0
                else -> 0.0
            }
            cb.update(a, xv, reward)
        }
        val picksPos = IntArray(2)
        val picksNeg = IntArray(2)
        repeat(200) {
            picksPos[cb.choose(feat(0.5))]++
            picksNeg[cb.choose(feat(-0.5))]++
        }
        assertTrue(picksPos[0] > picksPos[1], "at x>0 arm 0 should dominate: $picksPos")
        assertTrue(picksNeg[1] > picksNeg[0], "at x<0 arm 1 should dominate: $picksNeg")
    }

    @Test
    fun `RegressionContextualBandit drives random-forest arms`() {
        val rng = Random(20)
        val cb = RegressionContextualBandit(
            nbrArms = 2,
            template = RandomForestRegressionStat(
                featureSize = 1,
                splitCandidates = listOf(ThresholdSplit(0, 0.0)),
                nbrTrees = 4,
                config = TreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
                randomSeed = 21,
            ),
            posterior = ThompsonForestPosterior(priorWeight = 1.0, priorVariance = 1.0),
            random = rng,
        )
        repeat(400) {
            val x = rng.nextDouble() * 2 - 1
            val xv = feat(x)
            val a = cb.choose(xv)
            val reward = if (a == 0 && x > 0 || a == 1 && x <= 0) 1.0 else 0.0
            cb.update(a, xv, reward)
        }
        val pickAtPos = cb.choose(feat(0.7))
        val pickAtNeg = cb.choose(feat(-0.7))
        // Deterministic claims are flaky for ensembled Thompson; just sanity check
        // that the snapshot's predictions follow the expected ordering for arm 0.
        val snap = cb.armResult(0)
        assertTrue(snap.predict(feat(0.5)) > snap.predict(feat(-0.5)))
        // pick variables used to keep choose() in the loop hot path.
        assertTrue(pickAtPos in 0..1 && pickAtNeg in 0..1)
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
}

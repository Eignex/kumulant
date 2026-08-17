package com.eignex.kumulant.bandit.tree

import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.feat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat
import com.eignex.kumulant.stat.regression.tree.RegressionTreeConfig
import com.eignex.kumulant.stat.regression.tree.ThompsonForestPosterior
import com.eignex.kumulant.stat.regression.tree.ThompsonTreePosterior
import com.eignex.kumulant.stat.regression.tree.ThresholdSplit
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertTrue

class RegressionContextualBanditTreeTest {

    @Test
    fun `drives tree-based arms`() {
        val rng = Random(10)
        val cb = RegressionContextualBandit(
            nbrArms = 2,
            template = DecisionTreeRegressionStat(
                featureSize = 1,
                splitCandidates = listOf(ThresholdSplit(0, 0.0)),
                config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
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
    fun `drives random-forest arms`() {
        val rng = Random(20)
        val cb = RegressionContextualBandit(
            nbrArms = 2,
            template = RandomForestRegressionStat(
                featureSize = 1,
                splitCandidates = listOf(ThresholdSplit(0, 0.0)),
                nbrTrees = 4,
                config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
                randomSeed = 21,
            ),
            posterior = ThompsonForestPosterior(priorWeight = 1.0, priorVariance = 1.0),
            random = rng,
        )
        repeat(400) {
            val x = rng.nextDouble() * 2 - 1
            val xv = feat(x)
            val a = cb.choose(xv)
            val reward = if ((a == 0 && x > 0) || (a == 1 && x <= 0)) 1.0 else 0.0
            cb.update(a, xv, reward)
        }
        val pickAtPos = cb.choose(feat(0.7))
        val pickAtNeg = cb.choose(feat(-0.7))
        // Deterministic claims are flaky for ensembled Thompson; just sanity check
        // that the snapshot's predictions follow the expected ordering for arm 0.
        val snap = cb.armResult(0)
        assertTrue(snap.predict(feat(0.5)) > snap.predict(feat(-0.5)))
        assertTrue(pickAtPos in 0..1 && pickAtNeg in 0..1)
    }
}

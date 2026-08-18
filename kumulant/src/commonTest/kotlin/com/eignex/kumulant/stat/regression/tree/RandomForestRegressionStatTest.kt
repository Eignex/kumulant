package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.feat
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RandomForestRegressionStatTest {

    @Test
    fun `predict averages across trees`() {
        val stat = RandomForestRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            nbrTrees = 4,
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
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
    fun `merge requires matching forest size`() {
        val a = RandomForestRegressionStat(1, emptyList(), nbrTrees = 3, randomSeed = 6)
        val b = RandomForestRegressionStat(1, emptyList(), nbrTrees = 5, randomSeed = 7)
        assertFailsWith<IllegalArgumentException> { a.merge(b.read(0L)) }
    }

    @Test
    fun `snapshot totalWeights sums per-tree weights under bagging`() {
        val stat = RandomForestRegressionStat(
            featureSize = 1,
            splitCandidates = emptyList(),
            nbrTrees = 5,
            bagging = true,
            randomSeed = 100,
        )
        repeat(1000) { stat.update(feat(0.0), 1.0) }
        val snap = stat.read(0L)
        assertTrue(snap.totalWeights > 4_000.0, "totalWeights=${snap.totalWeights}")
        assertTrue(snap.totalWeights < 6_000.0, "totalWeights=${snap.totalWeights}")
    }

    @Test
    fun `without bagging gives identical per-tree totalWeights`() {
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

    @Test
    fun `reset restarts the bagging stream a fresh stat would draw`() {
        // reset promises the equivalent of a fresh stat, not merely an emptied one. The bagging draws
        // are the visible half of that: without restarting them the rebuilt forest resumes from
        // wherever the discarded one left off, and its per-tree weights never match a fresh stat's.
        val fresh = baggedForest(seed = 7)
        val reused = baggedForest(seed = 7)
        feed(reused, 400)
        reused.reset()

        feed(fresh, 400)
        feed(reused, 400)
        assertEquals(perTreeWeights(fresh), perTreeWeights(reused))
    }

    @Test
    fun `copies bag independently of the forest they came from`() {
        // An invariant guard rather than a regression test: copies drew distinct seeds before the
        // switch to CounterRandom too, since create advanced the parent's generator either way. What
        // it pins is that create must keep deriving per-copy seeds at all, because a window builds one
        // slice per copy and slices sharing a stream would bag every observation identically.
        val template = baggedForest(seed = 11)
        val first = template.create()
        val second = template.create()
        feed(first, 400)
        feed(second, 400)
        assertTrue(
            perTreeWeights(first) != perTreeWeights(second),
            "copies bagged identically: ${perTreeWeights(first)}",
        )
    }

    private fun baggedForest(seed: Int) = RandomForestRegressionStat(
        featureSize = 1,
        splitCandidates = emptyList(),
        nbrTrees = 4,
        bagging = true,
        randomSeed = seed,
    )

    // Per-tree weights rather than a prediction: bagging is exactly what they record, whereas a
    // forest's prediction converges to the same value however the observations were reweighted.
    private fun perTreeWeights(stat: RandomForestRegressionStat): List<Double> =
        stat.read(0L).trees.map { it.totalWeights }

    private fun feed(stat: RandomForestRegressionStat, n: Int) {
        repeat(n) { stat.update(feat(0.0), 1.0) }
    }
}

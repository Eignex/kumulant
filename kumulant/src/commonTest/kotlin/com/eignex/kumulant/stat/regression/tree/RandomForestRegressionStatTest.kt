package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.math.DenseVector
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RandomForestRegressionStatTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

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
}

package com.eignex.kumulant.stat.anomaly

import com.eignex.koblas.DenseVector
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HalfSpaceTreesStatTest {

    private val ranges = listOf(
        FeatureRange(0.0, 10.0),
        FeatureRange(0.0, 10.0),
    )

    @Test
    fun `anomalies score lower than inliers after the reference window is built`() {
        val stat = HalfSpaceTreesStat(
            featureSize = 2,
            featureRanges = ranges,
            numTrees = 12,
            height = 6,
            windowSize = 200,
            randomSeed = 7,
        )
        val rng = Random(11L)
        // Build up a reference window of inliers clustered around (3, 3).
        repeat(400) {
            stat.update(doubleArrayOf(3.0 + rng.nextDouble() * 0.5, 3.0 + rng.nextDouble() * 0.5))
        }
        val r = stat.read()
        val inlierScore = r.score(DenseVector.of(doubleArrayOf(3.2, 3.4)))
        val outlierScore = r.score(DenseVector.of(doubleArrayOf(9.0, 9.0)))
        // Outliers route to leaves with little reference-window mass.
        assertTrue(inlierScore > outlierScore, "inlier=$inlierScore outlier=$outlierScore")
    }

    @Test
    fun `read returns the immutable tree structure`() {
        val stat = HalfSpaceTreesStat(
            featureSize = 2,
            featureRanges = ranges,
            numTrees = 3,
            height = 2,
            windowSize = 10,
            randomSeed = 5,
        )
        val r = stat.read()
        assertEquals(3, r.numTrees)
        assertEquals(2, r.height)
        assertEquals(3 * 3, r.featureIndices.size) // numTrees * (2^height - 1)
        assertEquals(3 * 4, r.referenceMass.size) // numTrees * 2^height
        // All feature indices are within range.
        for (f in r.featureIndices) assertTrue(f in 0 until 2, "feature index out of range: $f")
        // All thresholds fall within configured per-feature ranges.
        for (i in r.thresholds.indices) {
            val f = r.featureIndices[i]
            val range = ranges[f]
            assertTrue(r.thresholds[i] in range.low..range.high, "threshold ${r.thresholds[i]} outside $range")
        }
    }

    @Test
    fun `merge with mismatched structure throws`() {
        val a = HalfSpaceTreesStat(featureSize = 2, featureRanges = ranges, randomSeed = 1)
        val b = HalfSpaceTreesStat(featureSize = 2, featureRanges = ranges, randomSeed = 2)
        repeat(300) { a.update(doubleArrayOf(1.0, 1.0)) }
        repeat(300) { b.update(doubleArrayOf(1.0, 1.0)) }
        assertFailsWith<IllegalArgumentException> { a.merge(b.read()) }
    }

    @Test
    fun `reset zeroes the masses`() {
        val stat = HalfSpaceTreesStat(featureSize = 2, featureRanges = ranges, windowSize = 5, randomSeed = 3)
        repeat(50) { stat.update(doubleArrayOf(2.0, 2.0)) }
        stat.reset()
        val r = stat.read()
        for (m in r.referenceMass) assertEquals(0.0, m)
        assertEquals(0.0, r.totalWeights)
    }
}

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class SplitMetricTest {

    private fun v(n: Double, mean: Double, variance: Double) =
        WeightedVarianceResult(totalWeights = n, mean = mean, variance = variance)

    @Test
    fun `VarianceReduction is zero when both sides match the parent`() {
        val parent = v(10.0, 0.0, 1.0)
        val pos = v(5.0, 0.0, 1.0)
        val neg = v(5.0, 0.0, 1.0)
        assertEquals(0.0, VarianceReduction.score(parent, pos, neg), 1e-9)
    }

    @Test
    fun `VarianceReduction is positive when the split separates variance`() {
        val parent = v(10.0, 0.0, 1.0)
        val pos = v(5.0, 1.0, 0.0)
        val neg = v(5.0, -1.0, 0.0)
        assertTrue(VarianceReduction.score(parent, pos, neg) > 0.0)
    }

    @Test
    fun `VarianceReduction guards zero-weight splits`() {
        val parent = v(0.0, 0.0, 0.0)
        val pos = v(0.0, 0.0, 0.0)
        val neg = v(0.0, 0.0, 0.0)
        assertEquals(0.0, VarianceReduction.score(parent, pos, neg))
    }

    @Test
    fun `rank picks the best split and reports runner-up`() {
        val total = v(20.0, 0.0, 1.0)
        // Candidate A: nice clean split. Candidate B: weaker.
        val posList = listOf(v(10.0, 1.0, 0.0), v(10.0, 0.4, 0.5))
        val negList = listOf(v(10.0, -1.0, 0.0), v(10.0, -0.4, 0.5))
        val ranked = VarianceReduction.rank(total, posList, negList, minSamplesSplit = 4.0, minSamplesLeaf = 2.0)
        assertEquals(0, ranked.bestIndex)
        assertTrue(ranked.top1 > ranked.top2)
        assertTrue(ranked.top2 > 0.0)
    }

    @Test
    fun `rank skips splits below minSamplesLeaf`() {
        val total = v(20.0, 0.0, 1.0)
        val posList = listOf(v(1.0, 5.0, 0.0)) // pos has only weight 1
        val negList = listOf(v(19.0, 0.0, 0.5))
        val ranked = VarianceReduction.rank(total, posList, negList, minSamplesSplit = 4.0, minSamplesLeaf = 2.0)
        assertEquals(-1, ranked.bestIndex)
    }

    @Test
    fun `rank skips splits below minSamplesSplit total`() {
        val total = v(20.0, 0.0, 1.0)
        val posList = listOf(v(2.0, 1.0, 0.0))
        val negList = listOf(v(2.0, -1.0, 0.0))
        val ranked = VarianceReduction.rank(total, posList, negList, minSamplesSplit = 10.0, minSamplesLeaf = 1.0)
        assertEquals(-1, ranked.bestIndex)
    }

    @Test
    fun `rank requires aligned pos and neg lists`() {
        assertFailsWith<IllegalArgumentException> {
            VarianceReduction.rank(v(1.0, 0.0, 1.0), listOf(v(1.0, 0.0, 0.0)), emptyList(), 0.0, 0.0)
        }
    }
}

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals

class ForestEvidenceTest {
    @Test
    fun `correlated duplicates preserve effective evidence`() {
        val evidence = WeightedVarianceResult(30.0, 2.0, 0.5)
        for (members in listOf(1, 2, 10)) {
            assertEquals(evidence, mergeForestEvidence(List(members) { evidence }))
        }
    }

    @Test
    fun `independent evidence retains total weight and between leaf variance`() {
        val pooled = mergeForestEvidence(
            listOf(WeightedVarianceResult(2.0, 1.0, 0.0), WeightedVarianceResult(6.0, 5.0, 0.0)),
            independent = true,
        )
        assertEquals(8.0, pooled.totalWeights)
        assertEquals(4.0, pooled.mean)
        assertEquals(3.0, pooled.variance)
    }

    @Test
    fun `empty members reduce effective weight without altering moments`() {
        val pooled = mergeForestEvidence(
            listOf(WeightedVarianceResult(10.0, 3.0, 2.0), WeightedVarianceResult(0.0, 0.0, 0.0)),
        )
        assertEquals(WeightedVarianceResult(5.0, 3.0, 2.0), pooled)
    }

    @Test
    fun `nonpositive evidence cannot corrupt positive siblings`() {
        val pooled = mergeForestEvidence(
            listOf(WeightedVarianceResult(4.0, 3.0, 2.0), WeightedVarianceResult(-4.0, -3.0, 2.0)),
        )
        assertEquals(WeightedVarianceResult(2.0, 3.0, 2.0), pooled)
    }
}

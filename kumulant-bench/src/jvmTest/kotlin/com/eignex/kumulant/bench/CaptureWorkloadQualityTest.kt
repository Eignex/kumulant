package com.eignex.kumulant.bench

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CaptureWorkloadQualityTest {
    @Test
    fun `rank error accounts for the entire interval occupied by ties`() {
        val input = doubleArrayOf(1.0, 1.0, 1.0, 2.0)

        assertEquals(0.0, empiricalRankError(input, 1.0, 0.5))
        assertEquals(0.15, empiricalRankError(input, 1.0, 0.9), 1e-12)
    }

    @Test
    fun `held out regression predictions match the seeded linear problem`() {
        val case = captureCases("smoke").single { it.id == "regression/update/4" }

        val result = workloadCaptureQuality(case)

        assertTrue(result.passed, result.metrics.toString())
        assertEquals("heldOutNormalizedMse", result.metrics.single().name)
    }

    @Test
    fun `owned snapshots remain unchanged after the source stat is updated`() {
        val case = captureCases("smoke").single { it.id == "storage/read/4" }

        val result = workloadCaptureQuality(case)

        assertTrue(result.passed, result.metrics.toString())
        assertEquals(0.0, result.metrics.single { it.name == "snapshotMutationError" }.value)
    }

    @Test
    fun `vector expressions agree with scalar arithmetic for every representation`() {
        val cases = selectCaptureCases("standard", "vectors", "*")

        for (case in cases) assertTrue(workloadCaptureQuality(case).passed, case.id)
    }

    @Test
    fun `bandit quality reports expected reward regret for each seed`() {
        val case = captureCases("smoke").single { it.id == "bandits/MultiArmed/Thompson/cycle" }.copy(batchSize = 32, qualitySeeds = listOf(0, 1))

        val result = workloadCaptureQuality(case)

        assertTrue(result.passed)
        assertEquals(2, result.metrics.count { it.name.startsWith("pseudoRegret/seed=") })
        assertTrue(result.metrics.all { it.value >= 0.0 })
    }

    @Test
    fun `sketch quality measures estimates rather than observation counters`() {
        for (case in selectCaptureCases("smoke", "sketches", "*")) {
            val result = workloadCaptureQuality(case)

            assertTrue(result.passed, result.metrics.toString())
            assertTrue(result.metrics.none { it.name.contains("totalSeen") })
        }
    }
}

package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.Workspace
import com.eignex.kumulant.bytesPerCall
import kotlin.test.Test
import kotlin.test.assertTrue

class BayesianWorkspaceAllocationTest {

    private fun populatedStat(): BayesianRegressionStat = BayesianRegressionStat(featureSize = 8).also { stat ->
        repeat(8) { i ->
            stat.update(doubleArrayOf(1.0, i.toDouble(), -0.5, 0.25, 2.0, -1.0, 0.75, 1.5), i.toDouble())
        }
    }

    @Test
    fun `workspace reuses merge vector scratch allocation`() {
        val merged = populatedStat().read()
        val allocated = populatedStat()
        val workspace = Workspace()
        val reused = populatedStat()

        val (allocatedBytes, workspaceBytes) = bytesPerCall(
            {
                allocated.reset()
                allocated.merge(merged)
            },
            {
                reused.reset()
                reused.merge(merged, workspace)
            },
        )

        assertTrue(
            workspaceBytes + 128.0 <= allocatedBytes,
            "workspace merge allocated $workspaceBytes B/merge versus $allocatedBytes B/merge",
        )
    }
}

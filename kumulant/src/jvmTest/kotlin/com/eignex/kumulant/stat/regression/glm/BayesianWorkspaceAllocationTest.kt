package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.Workspace
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class BayesianWorkspaceAllocationTest {

    private val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean

    private fun interface Body {
        fun run()
    }

    private fun bytesPerMerge(body: Body): Double {
        repeat(1_000) { body.run() }
        val id = Thread.currentThread().threadId()
        var best = Double.MAX_VALUE
        repeat(5) {
            val before = bean.getThreadAllocatedBytes(id)
            repeat(2_000) { body.run() }
            val after = bean.getThreadAllocatedBytes(id)
            best = minOf(best, (after - before).toDouble() / 2_000)
        }
        return best
    }

    private fun populatedStat(): BayesianRegressionStat = BayesianRegressionStat(featureSize = 8).also { stat ->
        repeat(8) { i ->
            stat.update(doubleArrayOf(1.0, i.toDouble(), -0.5, 0.25, 2.0, -1.0, 0.75, 1.5), i.toDouble())
        }
    }

    @Test
    fun `reserved workspace removes merge vector scratch allocation`() {
        val merged = populatedStat().read()
        val allocated = populatedStat()
        val workspace = Workspace().apply { reserve(8, 8) }
        val reused = populatedStat()

        val allocatedBytes = bytesPerMerge {
            allocated.reset()
            allocated.merge(merged)
        }
        val workspaceBytes = bytesPerMerge {
            reused.reset()
            reused.merge(merged, workspace)
        }

        assertTrue(
            workspaceBytes + 128.0 <= allocatedBytes,
            "workspace merge allocated $workspaceBytes B/merge versus $allocatedBytes B/merge",
        )
    }
}

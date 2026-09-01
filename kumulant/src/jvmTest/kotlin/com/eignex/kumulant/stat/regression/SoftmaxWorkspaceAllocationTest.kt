package com.eignex.kumulant.stat.regression

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseVector
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class SoftmaxWorkspaceAllocationTest {

    private val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean

    private fun interface Body {
        fun run()
    }

    private fun bytesPerCall(body: Body): Double {
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

    @Test
    fun `reserved workspace removes softmax update logits allocation`() {
        val allocating = SoftmaxRegressionStat(featureSize = 8, numClasses = 4, optimizer = Sgd(ConstantRate(0.05)))
        val reused = SoftmaxRegressionStat(featureSize = 8, numClasses = 4, optimizer = Sgd(ConstantRate(0.05)))
        val x = F64DenseVector.of(DoubleArray(8) { (it + 1).toDouble() / 8.0 })
        val workspace = Workspace().apply { reserve(4, 1) }

        val allocatedBytes = bytesPerCall { allocating.update(x, 1.0) }
        val workspaceBytes = bytesPerCall { reused.update(x, 1.0, workspace = workspace) }

        assertTrue(
            workspaceBytes + 32.0 <= allocatedBytes,
            "workspace update allocated $workspaceBytes B/call versus $allocatedBytes B/call",
        )
    }
}

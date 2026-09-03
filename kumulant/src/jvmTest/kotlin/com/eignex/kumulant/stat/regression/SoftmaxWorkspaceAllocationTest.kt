package com.eignex.kumulant.stat.regression

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseMatrix
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

    @Test
    fun `a reserved workspace removes the logits buffer softmax prediction otherwise allocates`() {
        val result = SoftmaxRegressionResult(
            featureSize = 2,
            numClasses = 3,
            weights = F64DenseMatrix.of(
                arrayOf(doubleArrayOf(1.0, 0.0), doubleArrayOf(0.0, 1.0), doubleArrayOf(-1.0, -1.0)),
            ),
            biases = F64DenseVector.of(doubleArrayOf(0.0, 0.0, 0.0)),
            totalWeights = 0.0,
            step = 0L,
            crossEntropy = 0.0,
        )
        val x = F64DenseVector.of(doubleArrayOf(1.0, 0.5))
        val workspace = Workspace().apply { reserve(3, 1) }

        val defaultBytes = bytesPerCall { result.predict(x) }
        val nullBytes = bytesPerCall { result.predict(x, null) }
        val workspaceBytes = bytesPerCall { result.predict(x, workspace) }

        // Scoring every class from one pass over x needs somewhere to put the logits, so without a
        // workspace that is one length-numClasses array per call. The workspace lends it instead.
        assertTrue(workspaceBytes <= 8.0, "workspace prediction allocated $workspaceBytes B/call")
        assertTrue(defaultBytes > workspaceBytes, "default prediction allocated $defaultBytes B/call")
        assertTrue(nullBytes > workspaceBytes, "null prediction allocated $nullBytes B/call")
    }
}

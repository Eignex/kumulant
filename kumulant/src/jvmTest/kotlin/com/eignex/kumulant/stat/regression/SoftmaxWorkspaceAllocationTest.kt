package com.eignex.kumulant.stat.regression

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.Workspace
import com.eignex.kumulant.assertAllocatesAtMost
import com.eignex.kumulant.bytesPerCall
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import kotlin.test.Test
import kotlin.test.assertTrue

class SoftmaxWorkspaceAllocationTest {

    @Test
    fun `workspace reuses softmax update logits allocation`() {
        val allocating = SoftmaxRegressionStat(featureSize = 8, numClasses = 4, optimizer = Sgd(ConstantRate(0.05)))
        val reused = SoftmaxRegressionStat(featureSize = 8, numClasses = 4, optimizer = Sgd(ConstantRate(0.05)))
        val x = DenseVector.of(DoubleArray(8) { (it + 1).toDouble() / 8.0 })
        val workspace = Workspace()

        val (allocatedBytes, workspaceBytes) = bytesPerCall(
            { allocating.update(x, 1.0) },
            { reused.update(x, 1.0, workspace = workspace) },
        )

        assertTrue(
            workspaceBytes + 32.0 <= allocatedBytes,
            "workspace update allocated $workspaceBytes B/call versus $allocatedBytes B/call",
        )
    }

    @Test
    fun `a workspace reuses the logits buffer softmax prediction otherwise allocates`() {
        val result = SoftmaxRegressionResult(
            featureSize = 2,
            numClasses = 3,
            weights = DenseMatrix.ofRows(
                arrayOf(doubleArrayOf(1.0, 0.0), doubleArrayOf(0.0, 1.0), doubleArrayOf(-1.0, -1.0)),
            ),
            biases = DenseVector.of(doubleArrayOf(0.0, 0.0, 0.0)),
            totalWeights = 0.0,
            step = 0L,
            crossEntropy = 0.0,
        )
        val x = DenseVector.of(doubleArrayOf(1.0, 0.5))
        val workspace = Workspace()

        val (defaultBytes, nullBytes, workspaceBytes) = bytesPerCall(
            { result.predict(x) },
            { result.predict(x, null) },
            { result.predict(x, workspace) },
        )

        // Scoring every class from one pass over x needs somewhere to put the logits, so without a
        // workspace that is one length-numClasses array per call. The workspace lends it instead.
        assertAllocatesAtMost(0.0, "workspace prediction") { result.predict(x, workspace) }
        assertTrue(workspaceBytes + 32.0 <= defaultBytes, "default prediction allocated $defaultBytes B/call")
        assertTrue(workspaceBytes + 32.0 <= nullBytes, "null prediction allocated $nullBytes B/call")
    }
}

package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.times
import com.eignex.kumulant.bytesPerCall
import kotlin.test.Test
import kotlin.test.assertTrue

class BayesianPriorInfoAllocationTest {

    @Test
    fun `destination prior information kernel avoids the materialized vector copy`() {
        val size = 128
        val precision = DenseMatrix.diagonal(size, 2.0)
        val mean = DenseVector.of(DoubleArray(size) { it * 0.01 })
        val destination = DoubleArray(size)
        var sink = 0.0

        val (materializedBytes, destinationBytes) = bytesPerCall(
            { sink += (precision * mean).toDoubleArray()[0] },
            {
                precision.multiplyInto(mean, destination)
                sink += destination[0]
            },
        )

        assertTrue(sink.isFinite())
        assertTrue(
            destinationBytes + 1_024.0 <= materializedBytes,
            "destination priorInfo allocated $destinationBytes B/run versus $materializedBytes B/run",
        )
    }
}

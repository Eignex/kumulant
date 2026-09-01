package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.times
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class BayesianPriorInfoAllocationTest {

    private val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean

    private fun interface Body {
        fun run()
    }

    private fun bytesPerRun(body: Body): Double {
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
    fun `destination prior information kernel avoids the materialized vector copy`() {
        val size = 128
        val precision = F64DenseMatrix.diagonal(size, 2.0)
        val mean = F64DenseVector.of(DoubleArray(size) { it * 0.01 })
        val destination = DoubleArray(size)
        var sink = 0.0

        val materializedBytes = bytesPerRun {
            sink += (precision * mean).toDoubleArray()[0]
        }
        val destinationBytes = bytesPerRun {
            precision.multiplyInto(mean, destination)
            sink += destination[0]
        }

        assertTrue(sink.isFinite())
        assertTrue(
            destinationBytes + 1_024.0 <= materializedBytes,
            "destination priorInfo allocated $destinationBytes B/run versus $materializedBytes B/run",
        )
    }
}

package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64DenseVector
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class GaussianNaiveBayesAllocationTest {

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
    fun `destination probabilities remove repeated class score allocation`() {
        val stat = GaussianNaiveBayesStat(featureSize = 32, numClasses = 4)
        repeat(16) { sample ->
            stat.update(DoubleArray(32) { feature -> (sample - feature).toDouble() * 0.1 }, (sample % 4).toDouble())
        }
        val result = stat.read()
        val x = F64DenseVector.of(DoubleArray(32) { it * 0.25 })
        val destination = DoubleArray(4)

        val allocatedBytes = bytesPerCall { result.probabilities(x) }
        val destinationBytes = bytesPerCall { result.probabilitiesInto(x, destination) }

        assertTrue(
            destinationBytes + 32.0 <= allocatedBytes,
            "destination probabilities allocated $destinationBytes B/call versus $allocatedBytes B/call",
        )
    }
}

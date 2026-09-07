package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64DenseVector
import com.eignex.kumulant.bytesPerCall
import kotlin.test.Test
import kotlin.test.assertTrue

class GaussianNaiveBayesAllocationTest {

    @Test
    fun `destination probabilities remove repeated class score allocation`() {
        val stat = GaussianNaiveBayesStat(featureSize = 32, numClasses = 4)
        repeat(16) { sample ->
            stat.update(DoubleArray(32) { feature -> (sample - feature).toDouble() * 0.1 }, (sample % 4).toDouble())
        }
        val result = stat.read()
        val x = F64DenseVector.of(DoubleArray(32) { it * 0.25 })
        val destination = DoubleArray(4)

        val (allocatedBytes, destinationBytes) = bytesPerCall(
            { result.probabilities(x) },
            { result.probabilitiesInto(x, destination) },
        )

        assertTrue(
            destinationBytes + 32.0 <= allocatedBytes,
            "destination probabilities allocated $destinationBytes B/call versus $allocatedBytes B/call",
        )
    }
}

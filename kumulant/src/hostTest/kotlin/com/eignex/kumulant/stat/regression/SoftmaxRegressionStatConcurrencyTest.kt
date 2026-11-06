@file:Suppress("UndocumentedPublicClass", "UndocumentedPublicFunction", "FunctionNaming")

package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.runConcurrently
import kotlin.test.Test
import kotlin.test.assertTrue

class SoftmaxRegressionStatConcurrencyTest {

    @Test
    fun `concurrent updates neither throw nor stop the stream being counted`() {
        // Shared None stats are outside the concurrency contract.
        for (concurrency in listOf(Concurrency.Relaxed, Concurrency.Strict, Concurrency.HighWrite)) {
            val stat = SoftmaxRegressionStat(featureSize = 3, numClasses = 3, concurrency = concurrency)

            runConcurrently(threads = 4, iterationsPerThread = 50) { thread, i ->
                stat.update(DoubleArray(3) { (thread + it) * 0.25 }, (i % 3).toDouble())
            }

            assertTrue(stat.totalWeights > 0.0, "$concurrency counted no observations")
        }
    }

    @Test
    fun `the relaxed level owns no shared scratch`() {
        val stat = SoftmaxRegressionStat(featureSize = 3, numClasses = 3, concurrency = Concurrency.Relaxed)

        runConcurrently(threads = 8, iterationsPerThread = 100) { thread, i ->
            stat.update(DoubleArray(3) { (thread + it) * 0.125 }, (i % 3).toDouble())
        }
    }
}

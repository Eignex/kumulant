package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.runConcurrently
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SoftmaxRegressionStatConcurrencyTest {

    @Test
    fun `concurrent updates neither throw nor stop the stream being counted`() {
        // Shared None stats are outside the concurrency contract.
        for (concurrency in listOf(Concurrency.Relaxed, Concurrency.Strict, Concurrency.HighWrite)) {
            val stat = SoftmaxRegressionStat(featureSize = 3, numClasses = 3, concurrency = concurrency)
            val failures = ConcurrentLinkedQueue<Throwable>()

            runConcurrently(threads = 4, iterationsPerThread = 50) { thread, i ->
                try {
                    stat.update(DoubleArray(3) { (thread + it) * 0.25 }, (i % 3).toDouble())
                } catch (error: Throwable) {
                    failures += error
                }
            }

            assertEquals(emptyList(), failures.toList(), "$concurrency update threw")
            assertTrue(stat.totalWeights > 0.0, "$concurrency counted no observations")
        }
    }

    @Test
    fun `the relaxed level owns no shared scratch`() {
        val stat = SoftmaxRegressionStat(featureSize = 3, numClasses = 3, concurrency = Concurrency.Relaxed)
        val failures = ConcurrentLinkedQueue<Throwable>()

        runConcurrently(threads = 8, iterationsPerThread = 100) { thread, i ->
            try {
                stat.update(DoubleArray(3) { (thread + it) * 0.125 }, (i % 3).toDouble())
            } catch (error: Throwable) {
                failures += error
            }
        }

        assertEquals(emptyList(), failures.toList(), "relaxed update threw")
    }
}

package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.runConcurrently
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// The update path borrows its logits from a workspace the stat owns, but only where the lock is real.
// `welfordLock` gives Relaxed a no-op, so that level must keep allocating; a shared workspace there would
// surface as a throw out of the linear-algebra layer rather than as the drift Relaxed promises.
class SoftmaxRegressionStatConcurrencyTest {

    @Test
    fun `concurrent updates neither throw nor stop the stream being counted`() {
        // None is excluded on purpose: its own documentation puts a shared None stat outside the
        // contract, and racing one corrupts the plain cells long before any scratch is reached.
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
        // Relaxed is the one level whose lock does not serialise, so it must not hold a workspace to
        // share. Racing it is in contract there, and this is the configuration that would throw.
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

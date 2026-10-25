package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.runConcurrently
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BayesianRegressionStatConcurrencyTest {

    private fun snapshot(seed: Double): PrecisionRegressionResult {
        val source = BayesianRegressionStat(featureSize = 4)
        for (i in 0 until 8) {
            source.update(DoubleArray(4) { (i + it) * 0.125 + seed }, seed + i * 0.5)
        }
        return source.read()
    }

    @Test
    fun `concurrent merges under a locked level neither throw nor lose their factor`() {
        for (concurrency in listOf(Concurrency.Relaxed, Concurrency.Strict, Concurrency.HighWrite)) {
            val target = BayesianRegressionStat(featureSize = 4, concurrency = concurrency)
            val incoming = List(4) { snapshot(it * 0.25) }
            val failures = ConcurrentLinkedQueue<Throwable>()

            runConcurrently(threads = 4, iterationsPerThread = 25) { thread, _ ->
                try {
                    target.merge(incoming[thread])
                } catch (error: Throwable) {
                    failures += error
                }
            }

            assertEquals(emptyList(), failures.toList(), "$concurrency merge threw")
            val merged = target.read()
            for (i in 0 until 4) {
                assertTrue(merged.precisionL[i, i] > 0.0, "$concurrency left pivot $i non-positive")
            }
        }
    }

    @Test
    fun `concurrent updates under a locked level neither throw nor corrupt the factor`() {
        for (concurrency in listOf(Concurrency.Relaxed, Concurrency.Strict, Concurrency.HighWrite)) {
            val target = BayesianRegressionStat(featureSize = 4, concurrency = concurrency)
            val failures = ConcurrentLinkedQueue<Throwable>()

            runConcurrently(threads = 4, iterationsPerThread = 50) { thread, i ->
                try {
                    target.update(DoubleArray(4) { (thread + it) * 0.125 }, (i % 3) - 1.0)
                } catch (error: Throwable) {
                    failures += error
                }
            }

            assertEquals(emptyList(), failures.toList(), "$concurrency update threw")
            val read = target.read()
            for (i in 0 until 4) {
                assertTrue(read.precisionL[i, i] > 0.0, "$concurrency left pivot $i non-positive")
            }
        }
    }

    @Test
    fun `replicas do not share scratch on the update path either`() {
        // Independent None stats expose shared scratch that a per-stat lock could otherwise hide.
        val original = BayesianRegressionStat(featureSize = 4, concurrency = Concurrency.None)
        val replica = original.create(Concurrency.None)
        val failures = ConcurrentLinkedQueue<Throwable>()

        runConcurrently(threads = 2, iterationsPerThread = 100) { thread, i ->
            try {
                (if (thread == 0) original else replica)
                    .update(DoubleArray(4) { (thread + it) * 0.125 }, (i % 3) - 1.0)
            } catch (error: Throwable) {
                failures += error
            }
        }

        assertEquals(emptyList(), failures.toList(), "a replica shared update scratch with its origin")
    }

    @Test
    fun `a replica does not share the scratch of the stat it came from`() {
        val original = BayesianRegressionStat(featureSize = 4, concurrency = Concurrency.None)
        val replica = original.create(Concurrency.None)
        val incoming = snapshot(0.5)
        val failures = ConcurrentLinkedQueue<Throwable>()

        runConcurrently(threads = 2, iterationsPerThread = 25) { thread, _ ->
            try {
                (if (thread == 0) original else replica).merge(incoming)
            } catch (error: Throwable) {
                failures += error
            }
        }

        assertEquals(emptyList(), failures.toList(), "a replica shared scratch with its origin")
    }
}
